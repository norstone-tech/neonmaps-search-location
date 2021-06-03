const fs = require("fs");
const {promises: fsp} = require("fs");
const path = require("path");
/**
 * @typedef InternalTempData
 * @property {number} searchNum
 * @property {number} id
 * @property {0 | 1 | 2} type
 * @property {number} lonMin
 * @property {number} latMin
 * @property {number} lonMax
 * @property {number} latMax
 * @property {0 | 1 | 2} searchType 0 = within, 1 = intersect, 2 = envelop
 */
const INT16_SIZE = 2;
const INT24_SIZE = 3;
const INT32_SIZE = 4;
const INT40_SIZE = 5;
const INT48_SIZE = 6;
const sNew = Symbol("new");
class TempFileArray {
	/**
	 * @param {string} tmpdir
	 * @param {number} exponent
	 * @param {number} [cacheAmount=50]
	 */
	constructor(tmpdir, exponent, cacheAmount = 50){
		this._exponent = exponent;
		switch(exponent){
			case -2:
			case -1:
				this._searchNumSize = INT32_SIZE;
				break;
			case 0:
				this._searchNumSize = INT24_SIZE;
				break;
			case 1:
				this._searchNumSize = INT16_SIZE;
				break;
			default:
				throw new Error("No searchNumSize set for " + exponent);
		}
		this._bufLen = this._searchNumSize + INT48_SIZE + INT40_SIZE * 4 + 1;
		this.filePath = tmpdir + path.sep + exponent + ".tmp";
		this.length = 0;
		this.maxCacheAmount = cacheAmount;
		/**@type {Map<Number, InternalTempData>} */
		this._cache = new Map();
	}
	async init(){
		this.fd = await fsp.open(this.filePath, "w+");
	}
	async stop(){
		await this.fd.close();
		this.fd = null;
	}
	/**
	 * @param {Buffer} buf
	 * @returns {InternalTempData} 
	 */
	_bufToObj(buf){
		const lastByte = buf[buf.length - 1];
		const type = lastByte & 3;
		const searchType = (lastByte >> 2) & 3;
		return {
			searchNum: buf.readUIntLE(0, this._searchNumSize),
			id: buf.readUIntLE(this._searchNumSize, INT48_SIZE),
			type,
			lonMin: buf.readIntLE(
				this._searchNumSize + INT48_SIZE,
				INT40_SIZE
			),
			latMin: buf.readIntLE(
				this._searchNumSize + INT48_SIZE + INT40_SIZE,
				INT40_SIZE
			),
			lonMax: buf.readIntLE(
				this._searchNumSize + INT48_SIZE + INT40_SIZE * 2,
				INT40_SIZE
			),
			latMax: buf.readIntLE(
				this._searchNumSize + INT48_SIZE + INT40_SIZE * 3,
				INT40_SIZE
			),
			searchType
		};
	}
	/**
	 * @param {InternalTempData} obj
	 * @returns {Buffer}
	 */
	_objToBuf(obj){
		const buf = Buffer.allocUnsafe(this._bufLen);
		buf.writeUIntLE(obj.searchNum, 0, this._searchNumSize);
		buf.writeUIntLE(obj.id, this._searchNumSize, INT48_SIZE);
		buf.writeIntLE(obj.lonMin, this._searchNumSize + INT48_SIZE, INT40_SIZE);
		buf.writeIntLE(obj.latMin, this._searchNumSize + INT48_SIZE + INT40_SIZE, INT40_SIZE);
		buf.writeIntLE(obj.lonMax, this._searchNumSize + INT48_SIZE + INT40_SIZE * 2, INT40_SIZE);
		buf.writeIntLE(obj.latMax, this._searchNumSize + INT48_SIZE + INT40_SIZE * 3, INT40_SIZE);
		buf[buf.length - 1] = (obj.type) | (obj.searchType << 2);
		return buf;
	}
	/**
	 * @param {number} maxAmount
	 * @returns {Promise<void>}
	 */
	async flushCache(maxAmount = 0){
		while(this._cache.size > maxAmount){
			/**@type {number} */
			const i = this._cache.keys().next().value;
			const val = this._cache.get(i);
			this._cache.delete(i);
			if(val[sNew]){
				await this.setBuf(i, this._objToBuf(val));
			}
		}
	}
	/**
	 * @param {number} i
	 * @returns {Promise<Buffer>}
	 */
	async getBuf(i){
		const offset = i * this._bufLen;
		return (
			await this.fd.read(Buffer.allocUnsafe(this._bufLen), 0, this._bufLen, offset)
		).buffer;
	}
	/**
	 * @param {number} i 
	 * @param {Buffer} buf 
	 * @returns {Promise<void>}
	 */
	async setBuf(i, buf){
		const offset = i * this._bufLen;
		await this.fd.write(buf, 0, buf.length, offset);
	}

	/**
	 * @param {number} i
	 * @returns {Promise<InternalTempData>}
	 */
	async get(i){
		if(i < 0 || i >= this.length){
			return undefined;
		}
		if(this._cache.has(i)){
			const result = this._cache.get(i);
			this._cache.delete(i);
			this._cache.set(i, result);
			return result;
		}
		const result = this._bufToObj(await this.getBuf(i));
		this._cache.set(i, result);
		await this.flushCache(this.maxCacheAmount);
		return result;
	}
	/**
	 * @param {number} i 
	 * @param {InternalTempData} obj 
	 * @returns {Promise<void>}
	 */
	async set(i, obj){
		if(obj != this._cache.get(i)){
			obj[sNew] = true;
		}
		this._cache.set(i, obj);
		if(i >= this.length){
			this.length = i + 1;
		}
		await this.flushCache(this.maxCacheAmount);
	}
	/**
	 * @param {InternalTempData} obj 
	 * @returns {Promise<void>}
	 */
	push(obj){
		return this.set(this.length, obj);
	}
	async _shouldSwap(i, ii){
		const [val1, val2] = await Promise.all([
			this.get(i),
			this.get(ii)
		]);
		return val1.searchNum > val2.searchNum;
	}
	async _swap(i, ii){
		const [val1, val2] = await Promise.all([
			this.get(i),
			this.get(ii)
		]);
		val1[sNew] = true;
		val2[sNew] = true;
		this._cache.set(i, val2);
		this._cache.set(ii, val1);
		await this.flushCache(this.maxCacheAmount);
	}
	async _sortRecursively(startIndex = 0, endIndex = this.length - 1){
		/* Quick sort adapted from sort-algorithms-js Copyright (C) 2020 Eyas Ranjous <eyas.ranjous@gmail.com> used
		   under the MIT License */
		const pivotIndex = startIndex;
		let lowerIndex = startIndex;
		let higherIndex = endIndex;
		while (lowerIndex <= higherIndex) {
			while (await this._shouldSwap(pivotIndex, lowerIndex) && lowerIndex < endIndex) {
				lowerIndex += 1;
			}
			while (!await this._shouldSwap(pivotIndex, higherIndex) && higherIndex > startIndex) {
				higherIndex -= 1;
			}
			if (lowerIndex <= higherIndex) {
				await this._swap(lowerIndex, higherIndex);
				lowerIndex += 1;
				higherIndex -= 1;
			}
		}
		if (startIndex < higherIndex) {
			await this._sortRecursively(startIndex, higherIndex);
		}
	
		if (lowerIndex < endIndex) {
			console.log(
				"sort " + this._exponent + " " + lowerIndex + "/" + this.length +
				" (" + (lowerIndex / this.length * 100).toFixed(2) + "%)"
			);
			await this._sortRecursively(lowerIndex, endIndex);
		}
	}
	sort(){
		return this._sortRecursively();
	}
}
module.exports = {TempFileArray};
