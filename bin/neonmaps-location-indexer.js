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
const {logProgressMsg} = require("../lib/util");
const {setTempFileDir, getTempFile, flushTempFiles} = require("../lib/indexer/tmpfiles");

/**@typedef {import("neonmaps-base/lib/map-reader-base").OSMDecodeResult} OSMDecodeResult */
const MIN_INDEX_GRANULARITY = -2;
const MAX_INDEX_GRANULARITY = 1;
const NANO_EXPONENT = 9;
const NANO_DIVISOR = 10**9;
const INT48_SIZE = 6;
const options = program
	.requiredOption("-m, --map <path>", "Map file, in .osm.pbf format")
	.option(
		"--no-ignore-tagless",
		"Tag-less elements are ignored by default as they are assumed to be part of other geometry"
	)
	.parse()
	.opts();

const mapPath = path.resolve(options.map);
const mapReader = new MapReader(mapPath, 5, 5, 0, 10, 5, true, false);
(async () => {
	const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), "neonmaps-location-"));
	try{
		setTempFileDir(tmpDir);
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
				/* Using BigInt is the only way to truncate during division while making sure no rounding shenanigans are
				   going on */
				const nanoLon = BigInt(Math.round(node.lon * NANO_DIVISOR));
				const nanoLat = BigInt(Math.round(node.lat * NANO_DIVISOR));
				const granularityDivisor = BigInt(10 ** (NANO_EXPONENT + MIN_INDEX_GRANULARITY));
				const baseLon = Number(nanoLon / BigInt(NANO_DIVISOR));
				const baseLat = Number(nanoLat / BigInt(NANO_DIVISOR));
				const granularityLon = Number(nanoLon / granularityDivisor);
				const granularityLat = Number(nanoLat / granularityDivisor);
				const tmpData = await getTempFile(baseLon, baseLat, MIN_INDEX_GRANULARITY);
				/**@type {number} */
				const firstIndex = bounds.ge(tmpData.lon, granularityLon);
				/**@type {number} */
				const lastIndex = firstIndex >= tmpData.lon.length ? -1 : bounds.le(tmpData.lon, granularityLon);
				let index = firstIndex;
				if(firstIndex < tmpData.lon.length){
					const latSubarray = tmpData.lat.slice(firstIndex, lastIndex);
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
		/*
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
				const geometry = thing.type == "way" ?
					await mapReader.getWayGeoJSON(thing.id) :
					await mapReader.getRelationGeoJSON(thing.id);
				
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
		*/
		await flushTempFiles(0);
	}catch(ex){
		console.error(ex);
		process.exitCode = 1;
	}
})();