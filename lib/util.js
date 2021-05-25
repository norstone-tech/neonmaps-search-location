let nextProgressMsg = Date.now();
const logProgressMsg = function(...msg){
	if(nextProgressMsg <= Date.now()){
		console.log(...msg);
		nextProgressMsg = Date.now() + 300;
	}
};
module.exports = {logProgressMsg};
