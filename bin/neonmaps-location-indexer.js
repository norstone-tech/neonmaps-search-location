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
const bounds = require("binary-search-bounds");
const {default: geoBBox} = require("@turf/bbox");
const {default: geoBBoxPoly} = require("@turf/bbox-polygon");
const {default: geoContains, doBBoxOverlap: geoBBoxContains} = require("@turf/boolean-contains");
const {default: geoIntersects} = require("@turf/boolean-intersects");
const {logProgressMsg, multiPolyContainsPoly} = require("../lib/util");
const {setTempFileDir, getTempFile, flushTempFiles} = require("../lib/indexer/tmpfiles");
const {SearchSquare: SearchSquareParser} = require("../lib/proto-defs");
const Pbf = require("pbf");

/**@typedef {import("neonmaps-base/lib/map-reader-base").OSMDecodeResult} OSMDecodeResult */
/**@typedef {import("../lib/proto-defs").ProtoSearchSquare} ProtoSearchSquare */
/**@typedef {import("../lib/proto-defs").ProtoSearchSquareMember} ProtoSearchSquareMember */
const MIN_INDEX_GRANULARITY = -2;
const MAX_INDEX_GRANULARITY = 1;
const NANO_EXPONENT = 9;
const NANO_DIVISOR = 10**9;
const SIZE_BBOX_RATIO = 5; // Element bbox must be this times smaller than the containing granularity index
const INT48_SIZE = 6;
const INT32_SIZE = 4;
const OSMTYPE_NODE = 0;
const OSMTYPE_WAY = 1;
const OSMTYPE_RELATION = 2;
const options = program
	.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
	.option(
		"--no-ignore-tagless",
		"Tag-less elements are ignored by default as they are assumed to be part of other geometry"
	)
	.option("--tmpdir <dir>", "(debug) temp folder to use during indexing")
	.option("--no-phase1", "(debug) skip phase 1")
	.option("--no-phase2", "(debug) skip phase 2")
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
(async () => {
	const tmpDir = options.tmpdir ?
		path.resolve(options.tmpdir) :
		await fsp.mkdtemp(path.join(os.tmpdir(), "neonmaps-location-"));
	try{
		setTempFileDir(tmpDir);
		await mapReader.init();
		if(options.phase1){
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
			for(mapDataPromise of mapReader.mapSegments()){
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
					const granularityDivisor = 10 ** MIN_INDEX_GRANULARITY;
					const baseLon = Math.floor(node.lon);
					const baseLat = Math.floor(node.lat);
					const granularityLon = Math.floor(node.lon / granularityDivisor);
					const granularityLat = Math.floor(node.lat / granularityDivisor);
					const tmpData = await getTempFile(baseLon, baseLat, MIN_INDEX_GRANULARITY);
					if(!tmpData.lon.every((v,i,a) => !i || a[i-1] <= v)){
						debugger;
					}
					/**@type {number} */
					const firstIndex = bounds.ge(tmpData.lon, granularityLon);
					/**@type {number} */
					const lastIndex = firstIndex >= tmpData.lon.length ? -1 : bounds.le(tmpData.lon, granularityLon);
					let index = firstIndex;
					// Note: I was dumb to think there would always be an LE, and that caused me much needless headache
					if(lastIndex >= 0){
						const latSubarray = tmpData.lat.slice(firstIndex, lastIndex + 1);
						if(!latSubarray.every((v,i,a) => !i || a[i-1] <= v)){
							debugger;
						}
						index += bounds.ge(latSubarray, granularityLat);
					}
					tmpData.id.splice(index, 0, node.id);
					tmpData.type.splice(index, 0, 0);
					tmpData.lon.splice(index, 0, granularityLon);
					tmpData.lat.splice(index, 0, granularityLat);
				}
				curentSegment += 1;
				logProgressMsg(
					"Node indexing: " + curentSegment + "/" + segmentCount + " (" +
					(curentSegment / segmentCount * 100).toFixed(2) +
					"%)"
				);
			}
			console.log("Node indexing: " + segmentCount + "/" + segmentCount + " (100%)");
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
					if(thing.tags.size == 0){
						continue;
					}
					const geometry = thing.type == "way" ?
						await mapReader.getWayGeoJSON(thing) :
						await mapReader.getRelationGeoJSON(thing);
					const bbox = geoBBox(geometry);
					// Math.abs is probably redundant here, but just to be
					const bbSize = Math.max(Math.abs(bbox[0] - bbox[2]), Math.abs(bbox[1] - bbox[3]));
					let exponent = Math.floor(Math.log10(bbSize * SIZE_BBOX_RATIO)) + 1;
					if(exponent < MIN_INDEX_GRANULARITY){
						exponent = MIN_INDEX_GRANULARITY;
					}else if(exponent > MAX_INDEX_GRANULARITY){
						exponent = MAX_INDEX_GRANULARITY;
					}
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
						for(
							let granularityLat = granularityMinLat;
							granularityLat <= granularityMaxLat;
							granularityLat += 1
						){
							const baseLon = Math.floor(granularityLon * granularityDivisor);
							const baseLat = Math.floor(granularityLat * granularityDivisor);
							const tmpData = await getTempFile(baseLon, baseLat, exponent);
							if(!tmpData.lon.every((v,i,a) => !i || a[i-1] <= v)){
								debugger;
							}
							/**@type {number} */
							const firstIndex = bounds.ge(tmpData.lon, granularityLon);
							/**@type {number} */
							const lastIndex = firstIndex >= tmpData.lon.length ? -1 : bounds.le(tmpData.lon, granularityLon);
							let index = firstIndex;
							if(lastIndex >= 0){
								const latSubarray = tmpData.lat.slice(firstIndex, lastIndex + 1);
								if(!latSubarray.every((v,i,a) => !i || a[i-1] <= v)){
									debugger;
								}
								index += bounds.ge(latSubarray, granularityLat);
							}
							tmpData.id.splice(index, 0, thing.id);
							tmpData.type.splice(index, 0, thing.type == "way" ? 1 : 2);
							tmpData.lon.splice(index, 0, granularityLon);
							tmpData.lat.splice(index, 0, granularityLat);
						}
					}
				}
				logProgressMsg(
					"way/relation indexing: " +
						(curentSegment - firstSegmentWithWay) + "/" + (segmentCount - firstSegmentWithWay) +
					" (" +
					((curentSegment - firstSegmentWithWay) / (segmentCount - firstSegmentWithWay) * 100).toFixed(2) +
					"%)"
				);
			}
			console.log(
				"way/relation indexing: " +
				(segmentCount - firstSegmentWithWay) + "/" + (segmentCount - firstSegmentWithWay) +
				" (100%)"
			);
			await flushTempFiles(0);
		}
		for(
			let exponent = options.phase2 ? MIN_INDEX_GRANULARITY : Infinity;
			exponent <= MAX_INDEX_GRANULARITY;
			exponent += 1
		){
			const tmpFileOffsets = fs.createWriteStream(tmpDir + "/" + exponent + "_offsets");
			const tmpFileProtoBufs = fs.createWriteStream(tmpDir + "/" + exponent + "_pbfs");
			let currentOffset = 0;

			const granularityDivisor = 10 ** exponent;
			const maxLon = 180 / granularityDivisor;
			const maxLat = 90 / granularityDivisor;
			for(let granularityLon = -180 / granularityDivisor; granularityLon < maxLon; granularityLon += 1){
				const uGranularityLon = granularityLon + (180 / granularityDivisor);
				let allNonexistant = true;
				for(let granularityLat = -90 / granularityDivisor; granularityLat < maxLat; granularityLat += 1){
					const uGranularityLat = granularityLat + (90 / granularityDivisor);
					const tmpData = await getTempFile(
						Math.floor(granularityLon * granularityDivisor),
						Math.floor(granularityLat * granularityDivisor),
						exponent
					);
					if(!tmpData.id.length){
						granularityLat = (
							(Math.floor(granularityLat * granularityDivisor) + 1
						) / granularityDivisor) - 1;
						continue;
					}
					allNonexistant = false;
					/**@type {number} */
					const firstLonIndex = bounds.ge(tmpData.lon, granularityLon);
					if(tmpData.lon[firstLonIndex] != granularityLon){
						break;
					}
					/**@type {number} */
					const lastLonIndex = bounds.le(tmpData.lon, granularityLon) + 1;
					const latSubarray = tmpData.lat.slice(firstLonIndex, lastLonIndex);
					/**@type {number} */
					const firstLatIndex = bounds.ge(latSubarray, granularityLat);
					if(latSubarray[firstLatIndex] != granularityLat){
						continue;
					}
					/**@type {number} */
					const lastLatIndex = bounds.le(latSubarray, granularityLat) + 1;
					const firstIndex = firstLonIndex + firstLatIndex;
					const lastIndex = firstLonIndex + lastLatIndex;
					const firstLon = granularityLon * 10 ** (NANO_EXPONENT + exponent);
					const firstLat = granularityLat * 10 ** (NANO_EXPONENT + exponent);
					const searchBBoxPoly = geoBBoxPoly([
						Number((firstLon / NANO_DIVISOR).toFixed(9)),
						Number((firstLat / NANO_DIVISOR).toFixed(9)),
						Number((((granularityLon + 1) * 10 ** (NANO_EXPONENT + exponent)) / NANO_DIVISOR).toFixed(9)),
						Number((((granularityLat + 1) * 10 ** (NANO_EXPONENT + exponent)) / NANO_DIVISOR).toFixed(9))
					]);
					const searchData = {
						within: {
							_lastID: 0,
							_lastLonMin: firstLon,
							_lastLatMin: firstLat,
							_lastLonMax: firstLon,
							_lastLatMax: firstLat,
							id: [],
							type: [],
							lonMin: [],
							latMin: [],
							lonMax: [],
							latMax: []
						},
						intersected: {
							_lastID: 0,
							_lastLonMin: firstLon,
							_lastLatMin: firstLat,
							_lastLonMax: firstLon,
							_lastLatMax: firstLat,
							id: [],
							type: [],
							lonMin: [],
							latMin: [],
							lonMax: [],
							latMax: []
						},
						enveloped: {
							_lastID: 0,
							_lastLonMin: firstLon,
							_lastLatMin: firstLat,
							_lastLonMax: firstLon,
							_lastLatMax: firstLat,
							id: [],
							type: [],
							lonMin: [],
							latMin: [],
							lonMax: [],
							latMax: []
						},
					}
					const subArrID = tmpData.id.slice(firstIndex, lastIndex);
					const subArrType = tmpData.type.slice(firstIndex, lastIndex);
					/*
					const subArrLon = tmpData.lon.slice(firstIndex, lastIndex);
					const subArrLat = tmpData.lon.slice(firstIndex, lastIndex);
					*/
					for(let i = 0; i < subArrID.length; i += 1){
						const id = subArrID[i];
						const type = subArrType[i];
						if(type == OSMTYPE_NODE){
							// Points are always fully within the square
							const node = await mapReader.getNode(id);
							const nanoLon = Math.round(node.lon * NANO_DIVISOR);
							const nanoLat = Math.round(node.lat * NANO_DIVISOR);
							const {within} = searchData;
							within.id.push(id - within._lastID);
							within._lastID = id;
							within.type.push(type);
							within.lonMin.push(nanoLon - within._lastLonMin);
							within._lastLonMin = nanoLon;
							within.latMin.push(nanoLat - within._lastLatMin);
							within._lastLatMin = nanoLat;
							within.lonMax.push(nanoLon - within._lastLonMax);
							within._lastLonMax = nanoLon;
							within.latMax.push(nanoLat - within._lastLatMax);
							within._lastLatMax = nanoLat;
							continue;
						}
						const osmElem = type == OSMTYPE_WAY ?
							await mapReader.getWay(id) :
							await mapReader.getRelation(id);
						const osmPoly = type == OSMTYPE_WAY ?
							await mapReader.getWayGeoJSON(osmElem) :
							await mapReader.getRelationGeoJSON(osmElem);
						osmPoly.bbox = geoBBox(osmPoly);
						const searchMember = geoBBoxContains(searchBBoxPoly.bbox, osmPoly.bbox) ?
							searchData.within : (
								geoIntersects(searchBBoxPoly, osmPoly) ?
									searchData.intersected : (
										(
											osmPoly.geometry.type !== "LineString" &&
											osmPoly.geometry.type !== "MultiLineString" &&
											multiPolyContainsPoly(osmPoly, searchBBoxPoly)
										) ? searchData.enveloped : null
									)
							);
						if(searchMember == null){
							// Bounding box may have overlapped, but the shape itself doesn't in any way
							continue;
						}
						const nanoMinLon = Math.round(osmPoly.bbox[0] * NANO_DIVISOR);
						const nanoMinLat = Math.round(osmPoly.bbox[1] * NANO_DIVISOR);
						const nanoMaxLon = Math.round(osmPoly.bbox[2] * NANO_DIVISOR);
						const nanoMaxLat = Math.round(osmPoly.bbox[3] * NANO_DIVISOR);
						searchMember.id.push(id - searchMember._lastID);
						searchMember._lastID = id;
						searchMember.type.push(type);
						searchMember.lonMin.push(nanoMinLon - searchMember._lastLonMin);
						searchMember._lastLonMin = nanoMinLon;
						searchMember.latMin.push(nanoMinLat - searchMember._lastLatMin);
						searchMember._lastLatMin = nanoMinLat;
						searchMember.lonMax.push(nanoMaxLon - searchMember._lastLonMax);
						searchMember._lastLonMax = nanoMaxLon;
						searchMember.latMax.push(nanoMaxLat - searchMember._lastLatMax);
						searchMember._lastLatMax = nanoMaxLat;
					}
					if(searchData.within._lastID == 0){
						delete searchData.within;
					}
					if(searchData.intersected._lastID == 0){
						delete searchData.intersected;
					}
					if(searchData.enveloped._lastID == 0){
						delete searchData.enveloped;
					}
					const searchPbf = new Pbf();
					SearchSquareParser.write(searchData, searchPbf);
					const searchBuf = searchPbf.finish();
					const indexBuf = Buffer.allocUnsafe(INT32_SIZE * 3);
					indexBuf.writeUInt32LE(uGranularityLon * 10 ** (-(exponent + 2)) + uGranularityLat);
					indexBuf.writeUInt32LE(currentOffset, INT32_SIZE);
					indexBuf.writeUInt32LE(searchBuf.length, INT32_SIZE * 2);
					currentOffset += searchBuf.length;
					await Promise.all([
						writeAndWait(tmpFileOffsets, indexBuf),
						writeAndWait(tmpFileProtoBufs, searchBuf)
					]);
				}
				if(allNonexistant){	
					granularityLon = (
						(Math.floor(granularityLon * granularityDivisor) + 1
					) / granularityDivisor) - 1;
				}
				logProgressMsg(
					"Index assembly part1 for 10**(" + exponent + "): " +
						(uGranularityLon) + "/" + (maxLon * 2) +
					" (" +
					(uGranularityLon / maxLon / 2 * 100).toFixed(2) +
					"%)"
				);
			}
			console.log(
				"Index assembly part1 for 10**(" + exponent + "): " + (maxLon * 2) + "/" + (maxLon * 2) + " (100%)"
			);
		}
	}catch(ex){
		console.error(ex);
		process.exitCode = 1;
	}
})();