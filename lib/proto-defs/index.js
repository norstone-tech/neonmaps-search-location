const fs = require("fs");
const protoCompile = require('pbf/compile');
const parseProtoSchema = require('protocol-buffers-schema');
const path = require("path");
/**
 * @typedef ProtoSearchSquareMember
 * @property {Array<number>} id
 * @property {Array<number>} type
 * @property {Array<number>} lonMin
 * @property {Array<number>} latMin
 * @property {Array<number>} lonMax
 * @property {Array<number>} latMax
 */
/**
 * @typedef ProtoSearchSquare
 * @property {ProtoSearchSquareMember} [within]
 * @property {ProtoSearchSquareMember} [intersected]
 * @property {ProtoSearchSquareMember} [enveloped]
 */
const {
	SearchSquare
} = protoCompile(
	parseProtoSchema(fs.readFileSync(path.resolve(__dirname, "neonmaps-location.proto")))
);
module.exports = {
	SearchSquare
};
