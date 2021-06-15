const fs = require("fs");
const {promises: fsp} =  require("fs");
const path = require("path");
const {MapReader} = require("neonmaps-base");
const {NumericIndexFileSearcher} = require("neonmaps-base/lib/index-readers/searcher");
const bounds = require("binary-search-bounds");
const {default: geoBBox} = require("@turf/bbox");
const {default: geoBBoxPoly} = require("@turf/bbox-polygon");
const {default: geoContains, doBBoxOverlap: geoBBoxContains} = require("@turf/boolean-contains");
const {default: geoIntersects} = require("@turf/boolean-intersects");
const {multiPolyContainsPoly, searchNumSizeFromExponent} = require("./util");
const {SearchSquare: SearchSquareParser} = require("../lib/proto-defs");
const Pbf = require("pbf");
const FILE_MAGIC_NUMBER = Buffer.from("neonmaps.location_index\0");
/**@typedef {import("../lib/proto-defs").ProtoSearchSquare} ProtoSearchSquare */
const FILE_CHECKSUM_LENGTH = 64;
const FILE_HEADER_SIZE = FILE_MAGIC_NUMBER.length + FILE_CHECKSUM_LENGTH + 3;
const INT24_SIZE = 3;
const INT48_SIZE = 6;
const NANO_EXP = 9;
const SEARCH_FLAGS_WITHIN = 1;
const SEARCH_FLAGS_INTERSECT = 2;
const SEARCH_FLAGS_ENVELOPING = 4;
const typeEnumToStr = ["node", "way", "relation"];
/**
 * @typedef LocationSearchResultMember
 * @property {number} id
 * @property {"node" | "way" | "relation"} type
 * @property {[number, number, number, number]} bbox 
 */
/**
 * @typedef LocationSearchResult
 * @property {Array<LocationSearchResultMember>} [within]
 * @property {Array<LocationSearchResultMember>} [intersect]
 * @property {Array<LocationSearchResultMember>} [enveloping]
 */
class LocationSearcher {
	/**
	 * 
	 * @param {MapReader} mapReader 
	 */
	constructor(mapReader){
		const mapName = mapReader.filePath.substring(
			mapReader.filePath.lastIndexOf(path.sep) + 1,
			mapReader.filePath.length - ".osm.pbf".length
		);
		this.mapReader = mapReader;
		this.filePath = path.resolve(mapReader.filePath, "..", mapName + ".neonmaps.location_index");
		
	}
	async init(){
		this.fd = await fsp.open(this.filePath);
		const fileHeader = (await this.fd.read(
			Buffer.allocUnsafe(FILE_HEADER_SIZE),
			0,
			FILE_HEADER_SIZE,
			0
		)).buffer;
		if(!fileHeader.slice(0, FILE_MAGIC_NUMBER.length).equals(FILE_MAGIC_NUMBER)){
			throw new Error("File is not an neonmaps.location_index file!");
		}
		if(
			this.mapReader.checksum &&
			!fileHeader.slice(
				FILE_MAGIC_NUMBER.length, FILE_MAGIC_NUMBER.length + FILE_CHECKSUM_LENGTH
			).equals(await this.mapReader.checksum)
		){
			throw new Error("Location index file doesn't match with map file!");
		}
		/**@type {Map<number, NumericIndexFileSearcher>} */
		this.searchers = new Map();
		let offset = FILE_MAGIC_NUMBER.length + FILE_CHECKSUM_LENGTH;
		this.minGranExp = fileHeader.readInt8(offset);
		offset += 1;
		this.maxGranExp = fileHeader.readInt8(offset);
		offset += 1;
		this.sizeBBoxRatio = fileHeader.readInt8(offset);
		offset += 1;
		for(let exponent = this.minGranExp; i <= this.maxGranExp; exponent += 1){
			const searchNumSize = searchNumSizeFromExponent(exponent);
			const boundOffsetsBuf = (await this.fd.read(
				Buffer.allocUnsafe(INT48_SIZE * 2),
				0,
				INT48_SIZE * 2,
				offset
			)).buffer;
			this.searchers.set(i, new NumericIndexFileSearcher(
				this.fd,
				searchNumSize + INT48_SIZE + INT24_SIZE,
				searchNumSize,
				0,
				boundOffsetsBuf.readUIntLE(0, INT48_SIZE),
				boundOffsetsBuf.readUIntLE(INT48_SIZE, INT48_SIZE)
			));
			offset += INT48_SIZE * 2;
		}
		this._protobufOffsetStart = this.searchers.get(this.maxGranExp).options.blockEnd;
	}
	/**
	 * @async
	 * @param {[number, number, number, number]} bbox
	 * @param {number} flags
	 * @param {number} [minWidth=0]
	 * @param {number} [minHeight=0]
	 * @param {number} [maxWidth=Infinity]
	 * @param {number} [maxHeight=Infinity]
	 * @returns {Promise<LocationSearchResult>}
	 */
	async searchBBox(bbox, flags, minWidth = 0, minHeight = 0, maxWidth = Infinity, maxHeight = Infinity){
		const bboxPoly = geoBBoxPoly(bbox);
		const searchWithin = flags & SEARCH_FLAGS_WITHIN > 0;
		const searchIntersect = flags & SEARCH_FLAGS_INTERSECT > 0;
		const searchEnveloping = flags & SEARCH_FLAGS_ENVELOPING > 0;
		/**@type {Array<number>} */
		const searchExponents = [];
		const uBBox = [
			bbox[0] + 180,
			bbox[1] + 90,
			bbox[2] + 180,
			bbox[3] + 90
		];
		for(let exponent = this.minGranExp; exponent <= this.maxGranExp; exponent += 1){
			const squareMinSize = exponent == this.minGranExp ? 0 : (10 ** (exponent - 1) / this.sizeBBoxRatio);
			if(squareMinSize > maxWidth || squareMinSize > maxHeight){
				continue;
			}
			const squareMaxSize = exponent == this.maxGranExp ? Infinity : (10 ** exponent / this.sizeBBoxRatio);
			if(squareMaxSize < minWidth || squareMaxSize < minHeight){
				continue;
			}
			searchExponents.push(exponent);
		}
		const alreadyExamined = new Set();
		/**@type {LocationSearchResult} */
		const result = {
			within: [],
			intersect: [],
			enveloping: []
		};
		await Promise.all(searchExponents.map(async (exponent) => {
			const searcher = this.searchers.get(exponent);
			const searchNumSize = searcher.options.intSize;
			// The whole toFixed stuff makes sure we get rid of any rounding errors
			const granBBox = exponent == 0 ?
				uBBox :
				uBBox.map(v => Number((v * 10 ** exponent).toFixed(NANO_EXP + exponent)));
			const minWithinX = Math.ceil(granBBox[0]); // Inclusive
			const minWithinY = Math.ceil(granBBox[1]); // Inclusive
			const maxWithinX = Math.floor(granBBox[2]); // Exclusive
			const maxWithinY = Math.floor(granBBox[3]); // Exclusive
			const minIntersectX = Math.floor(granBBox[0]); // Inclusive
			const minIntersectY = Math.floor(granBBox[1]); // Inclusive
			const maxIntersectX = Math.floor(granBBox[2]); // Inclusive
			const maxIntersectY = Math.floor(granBBox[3]); // Inclusive
			if(searchWithin || searchEnveloping){
				for(let withinX = minWithinX; withinX < maxWithinX; withinX += 1){
					for(let withinY = minWithinY; withinY < maxWithinY; withinY += 1){
						const searchNum = withinX * 10 ** ((-exponent) + 3) + withinY;
						const searchIndex = await searcher.eq(searchNum);
						if(searchIndex == -1){
							continue;
						}
						const searchBuf = await searcher.item(searchIndex);
						const pbfOffset = searchBuf.readUIntLE(searchNumSize, INT48_SIZE) + this._protobufOffsetStart;
						const pbfLength = searchBuf.readUIntLE(searchNumSize + INT48_SIZE, INT24_SIZE);
						/**@type {ProtoSearchSquare} */
						const searchSquare = SearchSquareParser.read(new Pbf(
							(await this.fd.read(Buffer.allocUnsafe(pbfLength), 0, pbfLength, pbfOffset)).buffer
						));
						if(searchWithin){
							if(searchSquare.within){
								let lastID = 0;
								let lastLonMin = Math.round(
									withinX * 10 ** (NANO_EXP + exponent) - 180 * 10 ** NANO_EXP
								);
								let lastLatMin = Math.round(
									withinY * 10 ** (NANO_EXP + exponent) - 90 * 10 ** NANO_EXP
								);
								let lastLonMax = lastLonMin;
								let lastLatMax = lastLatMin;
								for(let i = 0; i < searchSquare.within.id.length; i += 1){
									lastID += searchSquare.within.id[i];
									lastLonMin += searchSquare.within.lonMin[i];
									lastLatMin += searchSquare.within.latMin[i];
									lastLonMax += searchSquare.within.lonMax[i];
									lastLatMax += searchSquare.within.latMax[i];
									const width = Number(((lastLonMax - lastLonMin) / 10 ** NANO_EXP).toFixed(NANO_EXP));
									const height = Number(((lastLatMax - lastLatMin) / 10 ** NANO_EXP).toFixed(NANO_EXP));
									if(
										height >= minHeight &&
										height <= maxHeight &&
										width >= minWidth &&
										width <= maxWidth
									){
										result.within.push({
											id: lastID,
											type: typeEnumToStr[searchSquare.within.type[i]],
											bbox: [
												Number((lastLonMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
												Number((lastLatMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
												Number((lastLonMax / 10 ** NANO_EXP).toFixed(NANO_EXP)),
												Number((lastLatMax / 10 ** NANO_EXP).toFixed(NANO_EXP))
											]
										});
									}
								}
							}
							if(searchSquare.intersected){
								let lastID = 0;
								let lastLonMin = Math.round(
									withinX * 10 ** (NANO_EXP + exponent) - 180 * 10 ** NANO_EXP
								);
								let lastLatMin = Math.round(
									withinY * 10 ** (NANO_EXP + exponent) - 90 * 10 ** NANO_EXP
								);
								let lastLonMax = lastLonMin;
								let lastLatMax = lastLatMin;
								for(let i = 0; i < searchSquare.intersected.id.length; i += 1){
									const typeStr = typeEnumToStr[searchSquare.intersected.type[i]];
									lastID += searchSquare.intersected.id[i];
									lastLonMin += searchSquare.intersected.lonMin[i];
									lastLatMin += searchSquare.intersected.latMin[i];
									lastLonMax += searchSquare.intersected.lonMax[i];
									lastLatMax += searchSquare.intersected.latMax[i];
									const width = Number(
										((lastLonMax - lastLonMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
									);
									const height = Number(
										((lastLatMax - lastLatMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
									);
									if(
										width > maxWidth || width < minWidth ||
										height > maxHeight || height < minHeight
									){
										continue;
									}
									if(alreadyExamined.has(typeStr + "/" + lastID)){
										continue;
									}
									const itemBBox = [
										Number((lastLonMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
										Number((lastLatMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
										Number((lastLonMax / 10 ** NANO_EXP).toFixed(NANO_EXP)),
										Number((lastLatMax / 10 ** NANO_EXP).toFixed(NANO_EXP))
									];
									alreadyExamined.add(typeStr + "/" + lastID);
									if(geoBBoxContains(bbox, itemBBox)){
										result.within.push({
											id: lastID,
											type: typeStr,
											bbox: itemBBox
										});
									}
								}
							}
						}
						if(searchEnveloping && searchSquare.enveloped){
							let lastID = 0;
							let lastLonMin = Math.round(
								withinX * 10 ** (NANO_EXP + exponent) - 180 * 10 ** NANO_EXP
							);
							let lastLatMin = Math.round(
								withinY * 10 ** (NANO_EXP + exponent) - 90 * 10 ** NANO_EXP
							);
							let lastLonMax = lastLonMin;
							let lastLatMax = lastLatMin;
							for(let i = 0; i < searchSquare.enveloped.id.length; i += 1){
								const typeStr = typeEnumToStr[searchSquare.enveloped.type[i]];
								lastID += searchSquare.enveloped.id[i];
								lastLonMin += searchSquare.enveloped.lonMin[i];
								lastLatMin += searchSquare.enveloped.latMin[i];
								lastLonMax += searchSquare.enveloped.lonMax[i];
								lastLatMax += searchSquare.enveloped.latMax[i];
								const width = Number(
									((lastLonMax - lastLonMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
								);
								const height = Number(
									((lastLatMax - lastLatMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
								);
								if(
									width > maxWidth || width < minWidth ||
									height > maxHeight || height < minHeight
								){
									continue;
								}
								if(alreadyExamined.has(typeStr + "/" + lastID)){
									continue;
								}
								alreadyExamined.add(typeStr + "/" + lastID);
								/* We cannot just use bounding boxes to confirm envelopment because the lines may be
								   jaggaed around the bbox. So might as well just do the big check now and get on with
								   it */
								const geoJSON = typeStr == "way" ?
									await this.mapReader.getWayGeoJSON(lastID) :
									await this.mapReader.getRelationGeoJSON(lastID);
								if(geoJSON.geometry.type !== "Polygon" && geoJSON.geometry.type !== "MultiPolygon"){
									// Unclosed shapes cannot contain anything
									continue;
								}
								if(multiPolyContainsPoly(geoJSON, bboxPoly)){
									result.enveloping.push({
										id: lastID,
										type: typeStr,
										bbox: [
											Number((lastLonMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
											Number((lastLatMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
											Number((lastLonMax / 10 ** NANO_EXP).toFixed(NANO_EXP)),
											Number((lastLatMax / 10 ** NANO_EXP).toFixed(NANO_EXP))
										]
									});
								}
							}
						}
					}
				}
			}
			for(let intersectX = minIntersectX; intersectX <= maxIntersectX; intersectX += 1){
				for(let intersectY = minIntersectY; intersectY <= maxIntersectY; intersectY += 1){
					if(
						// The equality checks ensures the intersect search 
						(minWithinX == minIntersectX ? intersectX > minWithinX : intersectX >= minWithinX) &&
						(minWithinY == minIntersectY ? intersectY > minWithinY : intersectX >= minWithinY) &&
						intersectX < maxWithinX &&
						intersectY < maxWithinY
					){
						// We're in the "within" range, skip!
						continue;
					}
					const searchNum = intersectX * 10 ** ((-exponent) + 3) + intersectY;
					const searchIndex = await searcher.eq(searchNum);
					if(searchIndex == -1){
						continue;
					}
					const searchBuf = await searcher.item(searchIndex);
					const pbfOffset = searchBuf.readUIntLE(searchNumSize, INT48_SIZE) + this._protobufOffsetStart;
					const pbfLength = searchBuf.readUIntLE(searchNumSize + INT48_SIZE, INT24_SIZE);
					/**@type {ProtoSearchSquare} */
					const searchSquare = SearchSquareParser.read(new Pbf(
						(await this.fd.read(Buffer.allocUnsafe(pbfLength), 0, pbfLength, pbfOffset)).buffer
					));
					/**@type {Array<LocationSearchResultMember>} */
					const potentialResults = [];
					/* Within the intersect search squares, anything's game, so let's just shove everything into one
					   array to check */
					if(searchSquare.within){
						let lastID = 0;
						let lastLonMin = Math.round(
							intersectX * 10 ** (NANO_EXP + exponent) - 180 * 10 ** NANO_EXP
						);
						let lastLatMin = Math.round(
							intersectY * 10 ** (NANO_EXP + exponent) - 90 * 10 ** NANO_EXP
						);
						let lastLonMax = lastLonMin;
						let lastLatMax = lastLatMin;
						for(let i = 0; i < searchSquare.within.id.length; i += 1){
							const typeStr = typeEnumToStr[searchSquare.within.type[i]];
							lastID += searchSquare.within.id[i];
							lastLonMin += searchSquare.within.lonMin[i];
							lastLatMin += searchSquare.within.latMin[i];
							lastLonMax += searchSquare.within.lonMax[i];
							lastLatMax += searchSquare.within.latMax[i];
							const width = Number(
								((lastLonMax - lastLonMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
							);
							const height = Number(
								((lastLatMax - lastLatMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
							);
							if(
								width > maxWidth || width < minWidth ||
								height > maxHeight || height < minHeight
							){
								continue;
							}
							if(alreadyExamined.has(typeStr + "/" + lastID)){
								continue;
							}
							alreadyExamined.add(typeStr + "/" + lastID);
							potentialResults.push({
								id: lastID,
								type: typeEnumToStr[searchSquare.within.type[i]],
								bbox: [
									Number((lastLonMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLatMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLonMax / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLatMax / 10 ** NANO_EXP).toFixed(NANO_EXP))
								]
							});
						}
					}
					if(searchSquare.intersected){
						// TODO: Copy/pasting previous section is fugly
						let lastID = 0;
						let lastLonMin = Math.round(
							intersectX * 10 ** (NANO_EXP + exponent) - 180 * 10 ** NANO_EXP
						);
						let lastLatMin = Math.round(
							intersectY * 10 ** (NANO_EXP + exponent) - 90 * 10 ** NANO_EXP
						);
						let lastLonMax = lastLonMin;
						let lastLatMax = lastLatMin;
						for(let i = 0; i < searchSquare.intersected.id.length; i += 1){
							const typeStr = typeEnumToStr[searchSquare.intersected.type[i]];
							lastID += searchSquare.intersected.id[i];
							lastLonMin += searchSquare.intersected.lonMin[i];
							lastLatMin += searchSquare.intersected.latMin[i];
							lastLonMax += searchSquare.intersected.lonMax[i];
							lastLatMax += searchSquare.intersected.latMax[i];
							const width = Number(
								((lastLonMax - lastLonMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
							);
							const height = Number(
								((lastLatMax - lastLatMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
							);
							if(
								width > maxWidth || width < minWidth ||
								height > maxHeight || height < minHeight
							){
								continue;
							}
							if(alreadyExamined.has(typeStr + "/" + lastID)){
								continue;
							}
							alreadyExamined.add(typeStr + "/" + lastID);
							potentialResults.push({
								id: lastID,
								type: typeEnumToStr[searchSquare.within.type[i]],
								bbox: [
									Number((lastLonMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLatMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLonMax / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLatMax / 10 ** NANO_EXP).toFixed(NANO_EXP))
								]
							});
						}
					}
					if(searchSquare.enveloped){
						// TODO: Copy/pasting previous section is fugly
						let lastID = 0;
						let lastLonMin = Math.round(
							intersectX * 10 ** (NANO_EXP + exponent) - 180 * 10 ** NANO_EXP
						);
						let lastLatMin = Math.round(
							intersectY * 10 ** (NANO_EXP + exponent) - 90 * 10 ** NANO_EXP
						);
						let lastLonMax = lastLonMin;
						let lastLatMax = lastLatMin;
						for(let i = 0; i < searchSquare.enveloped.id.length; i += 1){
							const typeStr = typeEnumToStr[searchSquare.enveloped.type[i]];
							lastID += searchSquare.enveloped.id[i];
							lastLonMin += searchSquare.enveloped.lonMin[i];
							lastLatMin += searchSquare.enveloped.latMin[i];
							lastLonMax += searchSquare.enveloped.lonMax[i];
							lastLatMax += searchSquare.enveloped.latMax[i];
							const width = Number(
								((lastLonMax - lastLonMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
							);
							const height = Number(
								((lastLatMax - lastLatMin) / 10 ** NANO_EXP).toFixed(NANO_EXP)
							);
							if(
								width > maxWidth || width < minWidth ||
								height > maxHeight || height < minHeight
							){
								continue;
							}
							if(alreadyExamined.has(typeStr + "/" + lastID)){
								continue;
							}
							alreadyExamined.add(typeStr + "/" + lastID);
							potentialResults.push({
								id: lastID,
								type: typeEnumToStr[searchSquare.within.type[i]],
								bbox: [
									Number((lastLonMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLatMin / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLonMax / 10 ** NANO_EXP).toFixed(NANO_EXP)),
									Number((lastLatMax / 10 ** NANO_EXP).toFixed(NANO_EXP))
								]
							});
						}
					}
					for(let i = 0; i < potentialResults.length; i += 1){
						const potentialResult = potentialResults[i];
						if(searchWithin && geoBBoxContains(bbox, potentialResult.bbox)){
							result.within.push(potentialResult);
							continue;
						}
						if(potentialResult.type == "node"){
							continue;
						}
						let potentialResultGeoJSON;
						if(searchIntersect){
							if(
								(potentialResult.bbox[0] >= bbox[0] && potentialResult.bbox[2] <= bbox[2]) ||
								(potentialResult.bbox[1] >= bbox[1] && potentialResult.bbox[3] <= bbox[3])
							){
								// bbox is only intersecting with one of the lines, intersection is guaranteed
								result.intersect.push(potentialResult);
								continue;
							}
							// bbox is is intersecting at a corner... we need to actually check
							potentialResultGeoJSON = potentialResult.type == "way" ?
								await this.mapReader.getWayGeoJSON(lastID) :
								await this.mapReader.getRelationGeoJSON(lastID);
							if(geoIntersects(bboxPoly, potentialResultGeoJSON)){
								result.intersect.push(potentialResult);
								continue;
							}
						}
						if(searchEnveloping && geoBBoxContains(potentialResult.bbox, bbox)){
							// Enveloping isn't guaranteed from bboxes alone, so we need to check
							if(potentialResultGeoJSON == null){
								potentialResultGeoJSON = potentialResult.type == "way" ?
									await this.mapReader.getWayGeoJSON(lastID) :
									await this.mapReader.getRelationGeoJSON(lastID);
							}
							if(
								potentialResultGeoJSON.geometry.type !== "Polygon" &&
								potentialResultGeoJSON.geometry.type !== "MultiPolygon"
							){
								// Unclosed shapes cannot contain anything
								continue;
							}
							if(multiPolyContainsPoly(potentialResultGeoJSON, bboxPoly)){
								result.enveloping.push(potentialResult);
							}
						}
					}
				}
			}
		}));
		if(!searchWithin){
			delete result.within;
		}
		if(!searchIntersect){
			delete result.intersect;
		}
		if(!searchEnveloping){
			delete result.enveloping;
		}
		return result;
	}
}
module.exports = {LocationSearcher};
