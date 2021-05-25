const fs = require("fs");
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const path = require("path");
/**
 * @typedef InternalProtoTempFile
 * @property {Array<number>} id
 * @property {Array<number>} type
 * @property {Array<number>} lon
 * @property {Array<number>} lat
 */
const {
	TempFile
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "neonmaps-location-temp.proto")))
);
module.exports = {
	TempFile
};
