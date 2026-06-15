import { PresentationFile, FileBlob } from "@oai/artifact-tool";
const pres = await PresentationFile.importPptx(await FileBlob.load(process.argv[2]));
function protoInfo(obj,name){
  let p=obj; let i=0;
  while(p && i<4){ console.log(name,'proto',i,Object.getOwnPropertyNames(p)); p=Object.getPrototypeOf(p); i++; }
}
protoInfo(pres.slides,'slides');
console.log('slides own symbols', Object.getOwnPropertySymbols(pres.slides));
console.dir(pres.slides,{depth:3});
console.log('iterable', typeof pres.slides[Symbol.iterator]);
if (typeof pres.slides[Symbol.iterator]==='function') {
  let arr=[...pres.slides]; console.log('iter len',arr.length, arr[0]?.constructor?.name, Object.getOwnPropertyNames(arr[0]||{})); console.dir(arr[0],{depth:2});
}
