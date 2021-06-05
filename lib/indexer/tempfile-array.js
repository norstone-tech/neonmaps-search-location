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
class TempFileArray {
	/**
	 * @param {string} tmpdir
	 * @param {number} exponent
	 * @param {number} [sortBufSize=134217728]
	 */
	constructor(tmpdir, exponent, sortBufSize = 134217728){
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
		this._sortBufSize = sortBufSize;
		this._sortBufSize -= sortBufSize % this._bufLen;
		this.unsortFilePath = tmpdir + path.sep + exponent + "_unsorted.tmp";
		this.filePath = tmpdir + path.sep + exponent + ".tmp";
		this.length = 0;
		this._scratchpad = Buffer.allocUnsafe(this._bufLen);
	}
	async init(){
		try{
			this.fd = await fsp.open(this.filePath);
			this.length = (await this.fd.stat()).size / this._bufLen;
		}catch(ex){
			if(ex.code != "ENOENT"){
				throw ex;
			}
			this._unsortStream = fs.createWriteStream(this.unsortFilePath);
		}
		
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
	 * @returns {Promise<InternalTempData>}
	 */
	async get(i){
		if(i < 0 || i >= this.length){
			return undefined;
		}
		return this._bufToObj(await this.getBuf(i));
	}
	async push(obj){
		if(!this._unsortStream.write(this._objToBuf(obj))){
			await new Promise(resolve => this._unsortStream.once("drain", resolve));
		}
		this.length += 1;
	}
	/**
	 * @private
	 * @param {Buffer} buf 
	 * @param {number} i 
	 * @param {number} ii 
	 * @returns {boolean}
	 */
	_shouldSwap(buf, i, ii){
		return (
			i >= 0 &&
			ii >= 0 &&
			i < this.length &&
			ii < this.length &&
			(
				buf.readUIntLE(ii * this._bufLen, this._searchNumSize) >
				buf.readUIntLE(i * this._bufLen, this._searchNumSize)
			)
		);
	}
	/**
	 * @private
	 * @param {Buffer} buf 
	 * @param {number} i 
	 * @param {number} ii 
	 */
	_swap(buf, i, ii){
		const iStart = i * this._bufLen;
		const iEnd = (i + 1) * this._bufLen;
		const iiStart = ii * this._bufLen;
		const iiEnd = (ii + 1) * this._bufLen;
		buf.copy(this._scratchpad, 0, iiStart, iiEnd);
		buf.copyWithin(iiStart, iStart, iEnd);
		this._scratchpad.copy(buf, iStart);
	}
	/**
	 * @private
	 * @param {Buffer} buf 
	 * @param {number} startIndex 
	 * @param {number} endIndex
	 */
	_heapSort(buf){
		/* Heap sort adapted from sort-algorithms-js Copyright (C) 2020 Eyas Ranjous <eyas.ranjous@gmail.com> used
		   under the MIT License. Using heap sort here because quick sort resulted in a stack overflow! */
		const endIndex = buf.length / this._bufLen;
		/**
		 * calculates a parent's index from a child's index
		 * @param {number} childIndex
		 * @returns {number}
		 */
		const getParentIndex = (childIndex) => (
			Math.floor((childIndex - 1) / 2)
		);
		/**
		 * calculates the left child's index of a parent's index
		 * @param {number} parentIndex
		 * @returns {number}
		 */
		const getLeftChildIndex = (parentIndex) => (
			(parentIndex * 2) + 1
		);
		/**
		 * calculates the right child's index of a parent's index
		 * @param {number} parentIndex
		 * @returns {number}
		 */
		const getRightChildIndex = (parentIndex) => (
			(parentIndex * 2) + 2
		);
		/**
		 * bubbles an element at a position up in the heap
		 */
		const heapifyUp = (i) => {
			let childIndex = i;
			let parentIndex = getParentIndex(childIndex);

			while (this._shouldSwap(buf, parentIndex, childIndex)) {
				this._swap(buf, parentIndex, childIndex);
				childIndex = parentIndex;
				parentIndex = getParentIndex(childIndex);
			}
		};

		/**
		 * converts the array into a heap
		 */
		const heapify = () => {
			for (let i = 0; i < endIndex; i += 1) {
				heapifyUp(i);
			}
		};

		/**
		 * @param {number}
		 */
		const compareChildrenBefore = (i, leftIndex, rightIndex) => {
			if (this._shouldSwap(buf, leftIndex, rightIndex) && rightIndex < i) {
				return rightIndex;
			}
			return leftIndex;
		};

		/**
		 * pushes the swapped element with root down to its correct location
		 * @param {number} i - swapped node's index
		 */
		const heapifyDownUntil = (i) => {
			let parentIndex = 0;
			let leftIndex = 1;
			let rightIndex = 2;
			let childIndex;

			while (leftIndex < i) {
				childIndex = compareChildrenBefore(i, leftIndex, rightIndex);

				if (this._shouldSwap(buf, parentIndex, childIndex)) {
					this._swap(buf, parentIndex, childIndex);
				}

				parentIndex = childIndex;
				leftIndex = getLeftChildIndex(parentIndex);
				rightIndex = getRightChildIndex(parentIndex);
			}
		};
		heapify();
		for (let i = endIndex - 1; i > 0; i -= 1) {
			this._swap(buf, 0, i);
			heapifyDownUntil(i);
		}
	}
	async sort(){
		this._unsortStream.end();
		await new Promise(resolve => this._unsortStream.once("close", resolve));
		const chunkCount = Math.ceil(this.length * this._bufLen / this._sortBufSize);
		const unsortedFd = await fsp.open(this.unsortFilePath, "r+");
		const sortedBuckets = [];
		for(let i = 0; i < chunkCount; i += 1){
			const offset = this._sortBufSize * i;
			let {buffer, bytesRead} = await unsortedFd.read(
				Buffer.allocUnsafe(this._sortBufSize),
				0,
				this._sortBufSize,
				offset
			);
			if(bytesRead < buffer.length){
				buffer = buffer.slice(0, bytesRead);
			}
			this._heapSort(buffer);
			await unsortedFd.write(
				buffer,
				0,
				buffer.length,
				offset
			);
			/* Using slice wouldn't work here bcause Buffer.slice == Buffer.subarray, and we don't want to keep the
			   giant buffer */
			const scratchpad = Buffer.allocUnsafe(this._bufLen);
			buffer.copy(scratchpad, 0, 0, this._bufLen);
			sortedBuckets.push({
				offset,
				offsetBuf: scratchpad,
				offsetEnd: offset + buffer.length
			});
		}
		const sortedStream = fs.createWriteStream(this.filePath);		
		while(sortedBuckets.length){
			let smallestSearchNum = Infinity;
			let chosenIndex = -1;
			for(let i = 0; i < sortedBuckets.length; i += 1){
				const {offsetBuf} = sortedBuckets[i];
				const searchNum = offsetBuf.readUIntLE(0, this._searchNumSize);
				if(searchNum < smallestSearchNum){
					smallestSearchNum = searchNum;
					chosenIndex = i;
				}
			}
			const {offsetBuf, offsetEnd} = sortedBuckets[chosenIndex];
			if(!sortedStream.write(offsetBuf)){
				await new Promise(resolve => sortedStream.once("drain", resolve));
			}
			sortedBuckets[chosenIndex].offset += this._bufLen;
			const {offset} = sortedBuckets[chosenIndex];
			if(offset >= offsetEnd){
				sortedBuckets.splice(chosenIndex, 1);
			}else{
				/* New buffer must be allocated otherwise the data may be overwritten while it's still in the
				   sortedStream write queue */
				sortedBuckets[chosenIndex].offsetBuf = (
					await unsortedFd.read(Buffer.allocUnsafe(this._bufLen), 0, this._bufLen, offset)
				).buffer;
			}
		}
		sortedStream.end();
		await Promise.all([
			unsortedFd.close(),
			new Promise(resolve => sortedStream.once("close", resolve))
		]);
		this.fd = await fsp.open(this.filePath);
	}
}
module.exports = {TempFileArray};
