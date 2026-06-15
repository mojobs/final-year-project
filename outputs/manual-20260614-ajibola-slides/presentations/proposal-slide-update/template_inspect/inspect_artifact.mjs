import { PresentationFile, FileBlob } from "@oai/artifact-tool";
const pptx = process.argv[2];
const pres = await PresentationFile.importPptx(await FileBlob.load(pptx));
console.log('pres keys', Object.keys(pres));
console.log('slides type', pres.slides?.constructor?.name, 'len', pres.slides?.length, 'keys', pres.slides ? Object.keys(pres.slides).slice(0,20):null);
console.log('slide0 keys', pres.slides?.[0] ? Object.keys(pres.slides[0]) : null);
console.log('slide count?', pres.slides?.items?.length, pres.slides?.children?.length);
console.log('json', JSON.stringify(pres, (k,v)=> k==='blob'||k==='bytes' ? '[blob]' : v, 2).slice(0,3000));
