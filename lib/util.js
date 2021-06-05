const turf = require("@turf/helpers");
const {default: geoContains} = require("@turf/boolean-contains");
let nextProgressMsg = Date.now();
const logProgressMsg = function(...msg){
	if(nextProgressMsg <= Date.now()){
		console.log(...msg);
		nextProgressMsg = Date.now() + 300;
	}
};
const multiPolyContainsPoly = function(
	/**@type {turf.Feature<turf.Polygon | turf.MultiPolygon>}*/ poly1,
	/**@type {turf.Feature<turf.Polygon>}*/ poly2
){
	// poly2 must be within one of poly1's polys
	/**@type {Array<turf.Feature<turf.Polygon>>} */
	const polys1 = poly1.geometry.type != "MultiPolygon" ? [poly1] : poly1.geometry.coordinates.map(v => turf.polygon(v));
	let inPoly1 = false;
	for(let i = 0; i < polys1.length; i += 1){
		if(geoContains(polys1[i], poly2)){
			inPoly1 = true;
			break;
		}
	}
	return inPoly1;
}
const INT16_SIZE = 2;
const INT24_SIZE = 3;
const INT32_SIZE = 4;
const searchNumSizeFromExponent = function(exp){
	let result;
	switch(exp){
		case -2:
		case -1:
			result = INT32_SIZE;
			break;
		case 0:
			result = INT24_SIZE;
			break;
		case 1:
			result = INT16_SIZE;
			break;
		default:
			throw new Error("No searchNumSize set for " + i);
	}
	return result;
}
module.exports = {logProgressMsg, multiPolyContainsPoly, searchNumSizeFromExponent};
