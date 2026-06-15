import { PresentationFile, FileBlob } from "@oai/artifact-tool";
import fs from 'node:fs/promises';
async function summarize(pptx,out){
  const pres = await PresentationFile.importPptx(await FileBlob.load(pptx));
  const slides=[];
  for (const [idx,s] of pres.slides.items.entries()) {
    const elems=[];
    for (const e of s.elements.items) {
      const proto = e.toProto ? e.toProto() : {};
      const text = JSON.stringify(proto).match(/"text"\s*:\s*"([^"]*)"/g)?.map(x=>x.replace(/^"text"\s*:\s*"/,'').replace(/"$/,'')) || [];
      elems.push({id:e.id, type:e.constructor?.name, frame:e.frame, text:text.join(' | ').slice(0,500), protoKeys:Object.keys(proto)});
    }
    slides.push({slide:idx+1,id:s.id,elements:elems});
  }
  await fs.writeFile(out, JSON.stringify(slides,null,2));
  console.log('wrote', out);
}
await summarize(process.argv[2], process.argv[4]);
await summarize(process.argv[3], process.argv[5]);
