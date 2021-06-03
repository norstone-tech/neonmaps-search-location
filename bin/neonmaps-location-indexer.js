#!/usr/bin/env node
/* - Levels of indexed granularity: 10**1 to 10**-2
- bbox width/height (whichever's larger) (1/5th?) of degree
- get bounding box first, then see where it fits/intersects */
const os = require("os");
const path = require("path");
const fs = require("fs");
const {promises: fsp} = require("fs");
const {program} = require("commander");
const {MapReader} = require("neonmaps-base");
const {TempFileArray} = require("../lib/indexer/tempfile-array");
const bounds = require("binary-search-bounds");
const {default: geoBBox} = require("@turf/bbox");
const {default: geoBBoxPoly} = require("@turf/bbox-polygon");
const {default: geoContains, doBBoxOverlap: geoBBoxContains} = require("@turf/boolean-contains");
const {default: geoIntersects} = require("@turf/boolean-intersects");
const {logProgressMsg, multiPolyContainsPoly} = require("../lib/util");
const {SearchSquare: SearchSquareParser} = require("../lib/proto-defs");
const Pbf = require("pbf");

/**@typedef {import("neonmaps-base/lib/map-reader-base").OSMDecodeResult} OSMDecodeResult */
/**@typedef {import("../lib/proto-defs").ProtoSearchSquare} ProtoSearchSquare */
/**@typedef {import("../lib/proto-defs").ProtoSearchSquareMember} ProtoSearchSquareMember */
const MIN_INDEX_GRANULARITY = -2;
const MAX_INDEX_GRANULARITY = 1;
const SEARCH_TYPE_INVALID = -1;
const SEARCH_TYPE_WITHIN = 0;
const SEARCH_TYPE_INTERSECT = 1;
const SEARCH_TYPE_ENVELOPED = 2;
const OSMTYPE_NODE = 0;
const OSMTYPE_WAY = 1;
const OSMTYPE_RELATION = 2;
const NANO_EXPONENT = 9;
const NANO_DIVISOR = 10 ** 9;
const SIZE_BBOX_RATIO = 5;

const options = program
.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
.option(
	"--no-ignore-tagless",
	"Tag-less elements are ignored by default as they are assumed to be part of other geometry"
)
.option("--tmpdir <dir>", "(debug) temp folder to use during indexing")
.parse()
.opts();

const writeAndWait = function(
	/**@type {fs.WriteStream}*/ stream,
	/**@type {Buffer}*/ data
){
	if(!stream.write(data)){
		return new Promise(resolve => stream.once("drain", resolve));
	}
}
const mapPath = path.resolve(options.map);
const mapReader = new MapReader(mapPath, 5, 5, 0, 10, 5, true, false);
/**@type {Map<number, TempFileArray>} */
const tmpFiles = new Map();
(async () => {
	const tmpDir = options.tmpdir ?
		path.resolve(options.tmpdir) :
		await fsp.mkdtemp(path.join(os.tmpdir(), "neonmaps-location-"));
	try{
		await mapReader.init();
		/**@type {Promise<number>} */
		let offsetPromise;
		/**@type {Promise<OSMDecodeResult>} */
		let mapDataPromise;
		let curentSegment = 0;
		let segmentCount = 0;
		let firstSegmentWithWay = 0;
		for(offsetPromise of mapReader.offsets()){
			await offsetPromise;
			segmentCount += 1;
		}
		for(let i = MIN_INDEX_GRANULARITY; i <= MAX_INDEX_GRANULARITY; i += 1){
			const tmpFile = new TempFileArray(tmpDir, i);
			tmpFiles.set(i, tmpFile);
			await tmpFile.init();
		}
		for(mapDataPromise of mapReader.mapSegments()){
			const tmpFile = tmpFiles.get(MIN_INDEX_GRANULARITY);
			const mapData = await mapDataPromise;
			if(!firstSegmentWithWay && mapData.ways.length){
				firstSegmentWithWay = curentSegment;
			}
			if(!mapData.nodes.length){
				break;
			}
			for(let i = 0; i < mapData.nodes.length; i += 1){
				const node = mapData.nodes[i];
				if(options.ignoreTagless && node.tags.size == 0){
					continue;
				}
				const lon = Math.round(node.lon * NANO_DIVISOR);
				const lat = Math.round(node.lat * NANO_DIVISOR);
				const granularityDivisor = 10 ** MIN_INDEX_GRANULARITY;
				const granularityLon = Math.floor(node.lon / granularityDivisor);
				const granularityLat = Math.floor(node.lat / granularityDivisor);
				const uGranularityLon = granularityLon + (180 / granularityDivisor);
				const uGranularityLat = granularityLat + (90 / granularityDivisor);
				const searchNum = uGranularityLon * 10 ** ((-MIN_INDEX_GRANULARITY) + 3) + uGranularityLat;
				await tmpFile.push({
					searchNum,
					searchType: SEARCH_TYPE_WITHIN,
					id: node.id,
					type: OSMTYPE_NODE,
					lonMin: lon,
					latMin: lat,
					lonMax: lon,
					latMax: lat,
				});
			}
			curentSegment += 1;
			logProgressMsg(
				"Node resolving: " + curentSegment + "/" + segmentCount + " (" +
				(curentSegment / segmentCount * 100).toFixed(2) +
				"%)"
			);
		}
		console.log("Node resolving: " + segmentCount + "/" + segmentCount + " (100%)");
		curentSegment = 0;
		
		for(offsetPromise of mapReader.offsets()){
			const offset = await offsetPromise;
			curentSegment += 1;
			if(curentSegment < firstSegmentWithWay){
				continue;
			}
			const mapData = await mapReader.readDecodedMapSegment(offset);
			const things = [...mapData.ways, ...mapData.relations];
			for(let i = 0; i < things.length; i += 1){
				const thing = things[i];
				if(options.ignoreTagless && thing.tags.size == 0){
					continue;
				}
				const geometry = thing.type == "way" ?
					await mapReader.getWayGeoJSON(thing) :
					await mapReader.getRelationGeoJSON(thing);
				const bbox = geoBBox(geometry);
				const nanoBBox = bbox.map((v => Math.round(v * NANO_DIVISOR)));
				// Math.abs is probably redundant here, but it's good to be safe
				const bbSize = Math.max(Math.abs(bbox[0] - bbox[2]), Math.abs(bbox[1] - bbox[3]));
				let exponent = Math.floor(Math.log10(bbSize * SIZE_BBOX_RATIO)) + 1;
				if(exponent < MIN_INDEX_GRANULARITY){
					exponent = MIN_INDEX_GRANULARITY;
				}else if(exponent > MAX_INDEX_GRANULARITY){
					exponent = MAX_INDEX_GRANULARITY;
				}
				const tmpFile = tmpFiles.get(exponent);
				const granularityDivisor = 10 ** exponent;
				const granularityMinLon = Math.floor(bbox[0] / granularityDivisor);
				const granularityMinLat = Math.floor(bbox[1] / granularityDivisor);
				const granularityMaxLon = Math.floor(bbox[2] / granularityDivisor);
				const granularityMaxLat = Math.floor(bbox[3] / granularityDivisor);
				/* These for loops are needed in case this element is actually intersecting with multiple granularity
				indexes. This also conveniently skips over any elements with invalid geometry */
				for(
					let granularityLon = granularityMinLon;
					granularityLon <= granularityMaxLon;
					granularityLon += 1
				){
					const uGranularityLon = granularityLon + (180 / granularityDivisor);
					for(
						let granularityLat = granularityMinLat;
						granularityLat <= granularityMaxLat;
						granularityLat += 1
					){
						const uGranularityLat = granularityLat + (90 / granularityDivisor);
						const searchBBoxPoly = geoBBoxPoly([
							Number((
								(granularityLon * 10 ** (NANO_EXPONENT + exponent)) / NANO_DIVISOR
							).toFixed(9)),
							Number((
								(granularityLat * 10 ** (NANO_EXPONENT + exponent)) / NANO_DIVISOR
							).toFixed(9)),
							Number((
								((granularityLon + 1) * 10 ** (NANO_EXPONENT + exponent)) / NANO_DIVISOR
							).toFixed(9)),
							Number((
								((granularityLat + 1) * 10 ** (NANO_EXPONENT + exponent)) / NANO_DIVISOR
							).toFixed(9))
						]);
						const searchType = geoBBoxContains(searchBBoxPoly.bbox, bbox) ?
							SEARCH_TYPE_WITHIN : (
								geoIntersects(searchBBoxPoly, geometry) ?
									SEARCH_TYPE_INTERSECT : (
										(
											geometry.geometry.type !== "LineString" &&
											geometry.geometry.type !== "MultiLineString" &&
											multiPolyContainsPoly(geometry, searchBBoxPoly)
										) ? SEARCH_TYPE_ENVELOPED : SEARCH_TYPE_INVALID
									)
							);
						if(searchType == SEARCH_TYPE_INVALID){
							continue;
						}
						const searchNum = uGranularityLon * 10 ** ((-exponent) + 3) + uGranularityLat;
						await tmpFile.push({
							searchNum,
							searchType: SEARCH_TYPE_WITHIN,
							id: thing.id,
							type: thing.type == "way" ? OSMTYPE_WAY : OSMTYPE_RELATION,
							lonMin: nanoBBox[0],
							latMin: nanoBBox[1],
							lonMax: nanoBBox[2],
							latMax: nanoBBox[3],
						});
					}
				}
			}
			logProgressMsg(
				"way/relation resolving: " +
					(curentSegment - firstSegmentWithWay) + "/" + (segmentCount - firstSegmentWithWay) +
				" (" +
				((curentSegment - firstSegmentWithWay) / (segmentCount - firstSegmentWithWay) * 100).toFixed(2) +
				"%)"
			);
		}
		console.log(
			"way/relation resolving: " +
			(segmentCount - firstSegmentWithWay) + "/" + (segmentCount - firstSegmentWithWay) +
			" (100%)"
		);
		console.log("Index sorting: ?/? (0%)");
		await Promise.all([...tmpFiles.values()].map(tmpFile => tmpFile.sort()));
		console.log("Index sorting: ?/? (100%)");
	}catch(ex){
		console.error(ex);
		process.exitCode = 1;
	}
})();
