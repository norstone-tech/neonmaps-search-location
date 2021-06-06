const fs = require("fs");
const {promises: fsp} =  require("fs");
const path = require("path");
const {MapReader} = require("neonmaps-base");
const {NumericIndexFileSearcher} = require("neonmaps-base/lib/index-readers/searcher");
const {searchNumSizeFromExponent} = require("./util");
const FILE_MAGIC_NUMBER = Buffer.from("neonmaps.location_index\0");
const FILE_CHECKSUM_LENGTH = 64;
const FILE_HEADER_SIZE = FILE_MAGIC_NUMBER.length + FILE_CHECKSUM_LENGTH + 3;
const INT24_SIZE = 3;
const INT48_SIZE = 6;
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
}
module.exports = {LocationSearcher};
