import os
from pathlib import Path
import json
from fitz import Document as PyMuPDFDocument
from fitz import Pixmap, Matrix
from fitz import open as FitzOpen


from .parser import Parser, ParsedDocument, DocumentChunk, DocumentChunkRect

class PdfParser(Parser):
    def __init__(self, config:dict[str, any]):
        super().__init__(config)
        self.title_ignores = config.get('title-ignores') or ["journal", "volume", "issue", "page", "date", "doi", "abstract", "introduction", "conclusion", "acknowledgements", "references", "appendix", "figure", "table", "author", "editor", "reviewer", "keywords", "index", "bibliography", "publication", "submission", "correspondence", "contact", "about", "terms", "privacy", "policy", "license", "copyright"]
        self.save_images = config.get('save-images') or True
        self.image_output_folder = config.get('image-output-folder') or config.get('output-dir') or "images"
        self.image_output_folder = Path(self.image_output_folder)
        self.image_output_folder.mkdir(parents=True, exist_ok=True)

        self.image_prefix = config.get('image-prefix') or "img"
        self.drawings_prefix = config.get('drawing-prefix') or "drawing"
        self.min_image_side_length = int(config.get('min-image-side-length') or 100)  # each image side must be greater than this
        self.min_pixel_ratio = float(config.get('min-pixel-ratio') or 0.05)  # image : pixmap size ratio must be larger than this (5%)
        self.min_image_size = int(config.get('min-image-size') or 512)  # absolute image size limit (in bytes): ignore if smaller
        self.drawings_zoom = int(config.get('drawings-zoom') or 4)  # zoom factor for drawings

    def _parse(self, file:Path) -> ParsedDocument:
        # Open the PDF file
        pdf_document = FitzOpen(file)

        # Extract the images + drawings from the PDF
        img_list = self._extract_images_from_pdf(pdf_document, file.stem.replace(' ', '_'))

        # Extract the text from the PDF
        text_list = self._extract_text_from_pdf(pdf_document)

        pdf_document.close()
        
        chunks = []
        txt_ptr = 0
        img_ptr = 0
        
        while txt_ptr < len(text_list) or img_ptr < len(img_list):
            next_txt = None if txt_ptr >= len(text_list) else text_list[txt_ptr]
            next_img = None if img_ptr >= len(img_list) else img_list[img_ptr]

            if next_txt is None and next_img is None:
                break

            if next_txt is None:
                chunks.append(next_img)
                img_ptr += 1
            elif next_img is None:
                chunks.append(next_txt)
                txt_ptr += 1
            elif next_txt["page"] < next_img["page"]:
                chunks.append(next_txt)
                txt_ptr += 1
            elif next_txt["page"] > next_img["page"]:
                chunks.append(next_img)
                img_ptr += 1
            elif next_txt["bbox"]["y0"] < next_img["bbox"]["y0"]:
                chunks.append(next_txt)
                txt_ptr += 1
            else:
                chunks.append(next_img)
                img_ptr += 1
        
        parsed = ParsedDocument()
        parsed.chunks = []
        for i, chunk in enumerate(chunks):
            parsed_chunk = DocumentChunk()
            parsed_chunk.page = chunk["page"]
            parsed_chunk.page_chunk_idx = i
            parsed_chunk.rect = DocumentChunkRect(chunk["bbox"]["x0"], chunk["bbox"]["y0"], chunk["bbox"]["x1"], chunk["bbox"]["y1"])
            parsed_chunk.content = chunk["text"] if "text" in chunk else chunk["image"]
            parsed_chunk.metadata = chunk
            parsed_chunk.type = "text" if "text" in chunk else "image"
            parsed_chunk.link = f"{chunk['name']}" if "name" in chunk else None
            parsed.chunks.append(parsed_chunk)

            if (parsed.title is None or parsed.subtitle is None) and parsed_chunk.type == "text":
                text = chunk["text"]
                if len(text) > 0 and text.lower() not in self.title_ignores and ' ' in text: # Must be at least 2 words...
                    if parsed.title is None or len(parsed.title) == 0:
                        parsed.title = text[0: text.find("\n")].strip()
                    elif parsed.subtitle is None or len(parsed.subtitle) == 0:
                        parsed.subtitle = text[0: text.find("\n")].strip()
        return parsed


    def _extract_text_from_pdf(self, pdf_document:PyMuPDFDocument) -> list[dict]:
        text_list = list[dict]()
        for page_number in range(len(pdf_document)):
            page = pdf_document.load_page(page_number)
            # (x0, y0, x1, y1, "lines in the block", block_no, block_type)
            blocks = page.get_text("blocks", sort=False)    ## sort=True/False if needed
            for x0, y0, x1, y1, text, block_num, block_type in blocks:
                

                ## Find and new-line characters in the text. If the newline is preceded by the end of a sentence, then it's a new paragraph so include the newline, otherwise, it's likely midsentence so remove the newline
                pos = 0
                while pos < len(text):
                    pos = text.find("\n", pos)
                    if pos == -1: break
                    if pos > 0 and text[pos-1] in [".", "!", "?"]:
                        text = text[:pos] + "\n\n" + text[pos+1:]
                    else:
                    
                        if pos > 2 and text[pos-1].isdigit() and text[pos-2] == ".": ## If the newline is preceded by a period then a number, then it's likely a new sentence with the number being a reference, so include the newline
                            text = text[:pos] + "\n\n" + text[pos+1:]
                        elif pos > 0 and text[pos-1] in ['-', '—']: ## If the newline is preceded by a hyphen, then it's a broken word, so remove the newline + the hyphen
                            text = text[:pos-1] + text[pos+1:]
                        elif pos > 0 and text[pos-1] not in [" "]:  ## If the newline is not preceded by a space, then it's likely a word at the end of a line, so remove the newline + add a space
                            text = text[:pos] + " " + text[pos+1:]
                        else:
                            text = text[:pos] + text[pos+1:]
                    pos += 2

                # print(f"Block: {block_num}, Type: {block_type}, Rect: ({x0}, {y0}, {x1}, {y1})\nText: {text}")

                text_list.append({
                    "bbox": {
                        "x0": x0,
                        "y0": y0,
                        "x1": x1,
                        "y1": y1
                    },
                    "width": x1 - x0,
                    "height": y1 - y0,
                    "size": len(text),
                    "page": page_number,
                    "page_block_idx": block_num,
                    "text": text
                })
        
        return text_list

    def _extract_images_from_pdf(self, pdf_document:PyMuPDFDocument, image_file_prefix:str) -> list[dict]:
        ## iterate through all objects in the PDF, looking for images
        masks = set()  # Keep a reference to the Mask objects (for using with images that use the masks)
        already_processed = set()  # Don't double process xrefs
        img_list = list[dict]()
        for xref in range(1, pdf_document.xref_length()):
            # Skip already processed xrefs
            if xref in already_processed:
                continue

            # Only looking for images
            if pdf_document.xref_get_key(xref, "Subtype")[1] != "/Image":
                continue

            # Extact the xref as an Image            
            img_result = self._extract_image_by_xref(pdf_document, xref, masks)
            if not img_result["success"]:
                continue

            image_data = img_result["image"]
            ext = img_result["ext"]

            image_file_name = image_file_prefix + "_" + self.image_prefix + "-%i.%s" % (xref, ext)
            img_result["name"] = image_file_name
            if self.save_images:
                image_name = os.path.join(self.image_output_folder, image_file_name)
                with open(image_name, "wb") as ofile:
                    ofile.write(image_data)
                
                ## Write the metadata to a file
                drawing_meta_name = image_file_prefix + "_" + self.image_prefix + "-%i.metadata.json" % (xref)
                with open(f"{self.image_output_folder}/{drawing_meta_name}", "w") as f:
                    tmp = {}
                    for k, v in img_result.items(): 
                        if type(v) is bytes or k in ['image']:
                            continue
                        tmp[k] = v
                    json.dump(tmp, f, indent=4)
            
            already_processed.add(xref)
            img_list.append(img_result)


        # Iterate through each page, extract the drawings and save them
        for page_number in range(len(pdf_document)):
            page = pdf_document.load_page(page_number)
            
            ## Find all the drawings in the page
            bboxes = page.cluster_drawings()
            for i, bbox in enumerate(bboxes):
                pix = page.get_pixmap(clip=bbox, matrix=Matrix(self.drawings_zoom, self.drawings_zoom))
                drawing_name = f"{image_file_prefix}_{self.drawings_prefix}_{page_number+1}_{i+1}.png"
                drawing_data = {
                            "bbox": {
                                "x0": bbox[0],
                                "y0": bbox[1],
                                "x1": bbox[2],
                                "y1": bbox[3]
                            }, 
                            "image": pix.tobytes("png"),
                            "name": drawing_name,
                            "width": pix.width,
                            "height": pix.height,
                            "colorspace": pix.colorspace.name,
                            "ext": "png",
                            "size": pix.size,
                            "page": page_number,
                            "page_img_idx": i
                        }
                
                if self.save_images:
                    image_name = os.path.join(self.image_output_folder, drawing_name)
                    pix.save(image_name)

                    ## Write the metadata to a file
                    drawing_meta_name = f"{image_file_prefix}_{self.drawings_prefix}_{page_number+1}_{i+1}.metadata.json"
                    with open(f"{self.image_output_folder}/{drawing_meta_name}", "w") as f:
                        ## json.dump drawing_data without the image field
                        tmp = {}
                        for k, v in drawing_data.items(): 
                            if type(v) is bytes or k in ['image']:
                                continue
                            tmp[k] = v
                        json.dump(tmp, f, indent=4)

                        

                img_list.append(drawing_data)

        return img_list


    def _extract_image_by_xref(self, pdf_document:PyMuPDFDocument, xref, masks:set) -> dict:
        result = {
            "success": False,
            "reason": None,
            "image": None,
            "ext": None,
            "width": None,
            "height": None,
            "colorspace": None,
            "mask": None,
            "size": None,
            "page": None,
            "page_img_idx": None,
            "bbox": None
        }

        ## Not an image, so ignore
        if pdf_document.xref_get_key(xref, "Subtype")[1] != "/Image":
            result["reason"] = "Not an image"
            return result
        
        ## Check if the image is already in the mask set
        if xref in masks:
            result["reason"] = "Image is an mask, ignore it - it's a mask, we'll use the mask for loading other images"
            return result
        
        ## Extract the image
        image_data = pdf_document.extract_image(xref)
        if not image_data:  # Was unable to extract the image (or it wasn't really an image)
            result["reason"] = "Unable to extract image"
            return result

        ## If this is an mask, then store it in the masks set
        mask = image_data["smask"]
        if mask > 0:  # store /SMask xref
            masks.add(mask)
            result["mask"] = mask

        ## Check if the image is worth extracting by checking it's dimensions and size
        width = image_data["width"]
        result["width"] = width

        height = image_data["height"]
        result["height"] = height

        ext = image_data["ext"]
        result["ext"] = ext

        if min(width, height) <= self.min_image_side_length:  # rectangle edges too small
            result["reason"] = "Rectangle edges too small"
            return result

        ## Grab the image data
        data = image_data["image"]
        image_size = len(data)
        if image_size <= self.min_image_size:
            result["reason"] = "Image too small to be relevant"
            return result

        # If the image has a mask, then we need to create the pixmap with the mask applied
        if mask > 0:
            image_data = self._recoverpix(pdf_document, image_data)  # create pix with mask applied
            if image_data is None:  # something went wrong
                result["reason"] = "Something went wrong extracting image with mask"
                return result
            
            ext = "png"
            data = image_data["image"]
            num_samples = width * height * 3
            image_size = len(data)
        else:
            colourspace = max(1, image_data["colorspace"])  # get the colorspace n
            num_samples = width * height * colourspace  # simulated samples size

        if image_size / num_samples <= self.min_pixel_ratio:
            result["reason"] = "Unicolour image, doesn't seem to be worth extracting"
            return result

        result["image"] = data
        result["colourspace"] = image_data["colorspace"] if 'colorspace' in image_data else None
        result["size"] = image_size


        ## Go back through the PDF, looking for the page that this xref is within
        for page_number in range(len(pdf_document)):
            page = pdf_document.load_page(page_number)
            image_list = page.get_images(full=True)
            # Check for a matching image
            for image_index, img in enumerate(image_list):
                if img[0] == xref:      ## Index 0 is the xref
                    result["page"] = page_number
                    result["image_index"] = image_index
                    bbox = page.get_image_bbox(img[7])  # Index 7 is the image name
                    result["bbox"] = {
                        "x0": bbox[0],
                        "y0": bbox[1],
                        "x1": bbox[2],
                        "y1": bbox[3]                    
                    }
                    break
            
            if result["page"] is not None:
                break
        
        result["success"] = True
        return result


    def _recoverpix(self, doc:PyMuPDFDocument, image_data:dict):
        try:
            mask_xref = image_data["smask"]  # Get the xref of the image mask
            pix0 = Pixmap(image_data["image"])
            mask = Pixmap(doc.extract_image(mask_xref)["image"])
            pix = Pixmap(pix0, mask)
            if pix0.n > 3:
                ext = "pam"
            else:
                ext = "png"
            return {"ext": ext, "colorspace": pix.colorspace.n, "image": pix.tobytes(ext)}
        except:
            return None