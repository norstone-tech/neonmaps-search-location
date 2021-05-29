const path = require("path");
const fs = require("fs");
const {promises: fsp} = require("fs");
const {TempFile: TempFileParser} = require("../proto-defs");
const Pbf = require("pbf");
const MAX_CACHE_AMOUNT = 30;
/**@typedef {import("../proto-defs").InternalProtoTempFile} InternalProtoTempFile */
let tmpDir = "";
const setTempFileDir = function(str){
	tmpDir = str;
}
/**@type {Map<string, InternalProtoTempFile | Promise<InternalProtoTempFile>>} */
const cache = new Map();
/**@type {Map<string, Promise<void>>} */
const cacheWrites = new Map();
const getTempFile = async function(
	/**@type {number}*/ baseLon,
	/**@type {number}*/ baseLat,
	/**@type {number}*/ granExp
){
	const filePath = granExp >= 0 ? (granExp + ".pbf") : (granExp + path.sep + baseLon + "_" + baseLat + ".pbf");
	if(cache.has(filePath)){
		const result = cache.get(filePath);
		cache.delete(filePath);
		cache.set(filePath, result);
		return result;
	}
	const resultPromise = (async () => {
		if(granExp < 0){
			await fsp.mkdir(tmpDir + path.sep + granExp, {recursive: true});
		}
		/**@type {InternalProtoTempFile} */
		const data = await (async () => {
			try{
				return TempFileParser.read(new Pbf(await fsp.readFile(tmpDir + path.sep + filePath)));
			}catch{
				return {
					id: [],
					type: [],
					lon: [],
					lat: []
				};
			}
		})();
		return data;
	})();
	cache.set(filePath, resultPromise);
	flushTempFiles();
	const result = await resultPromise;
	cache.set(filePath, result);
	return result;
}
const flushTempFiles = function(maxCacheSize = MAX_CACHE_AMOUNT){
	while(cache.size > maxCacheSize){
		/**@type {string} */
		const filePath = cache.keys().next().value;
		const pbf = new Pbf();
		TempFileParser.write(cache.get(filePath), pbf);
		cacheWrites.set(
			filePath,
			fsp.writeFile(tmpDir + path.sep + filePath, pbf.finish()).then(() => {
				cacheWrites.delete(filePath);
			})
		);
		cache.delete(filePath);
	}
	return Promise.all(cacheWrites.values());
}
module.exports = {
	setTempFileDir,
	getTempFile,
	flushTempFiles
};
