import { PresentationFile, FileBlob } from "@oai/artifact-tool";
for (const pptx of process.argv.slice(2)) {
  const pres = await PresentationFile.importPptx(await FileBlob.load(pptx));
  console.log('\nPPTX', pptx);
  console.log('slides', pres.slides.items.length);
  pres.slides.items.forEach((s, idx) => {
    console.log('SLIDE', idx+1, 'keys', Object.getOwnPropertyNames(s), 'proto', Object.getOwnPropertyNames(Object.getPrototypeOf(s)));
    console.log(' slide id', s.id, 'elements?', s.elements?.items?.length, 'keys2', s.elements ? Object.getOwnPropertyNames(Object.getPrototypeOf(s.elements)) : null);
    if (idx<2) console.dir(s,{depth:2});
  });
}
