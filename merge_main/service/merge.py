import uuid
from typing import List
import fitz
import httpx
import asyncio
import configparser


config = configparser.ConfigParser()
config.read("config.ini")


dms_download_url = config["url_settings"]["dms_download_url"]
dms_upload_url = config["url_settings"]["dms_upload_url"]


async def download_pdf(pdf_dms_order: List[str]) -> List[httpx.Response]:
    """Downloads multiple PDFs asynchronously."""

    transport = httpx.AsyncHTTPTransport(retries=5)
    timeout = httpx.Timeout(timeout=3*60, connect=3*60, read=None, write=None)

    async with httpx.AsyncClient(transport=transport, timeout=timeout) as client:
        try:
            responses = await asyncio.gather(*[client.get(dms_download_url + dms) for dms in pdf_dms_order])
            return responses
        except httpx.HTTPError as e:
            print(f"Error downloading PDFs: {e}")
            return []


async def upload_pdf(pdf_path_list: List[str]) -> List[httpx.Response]:
    """Uploads multiple PDFs asynchronously."""
    transport = httpx.AsyncHTTPTransport(retries=5)
    timeout = httpx.Timeout(timeout=3*60, connect=3*60, read=None, write=None)

    async with httpx.AsyncClient(transport=transport, timeout=timeout) as client:
        try:
            responses = await asyncio.gather(*[client.post(dms_upload_url, files={'data': open(pdf_path, 'rb')}) for pdf_path in pdf_path_list])
            return responses
        except httpx.HTTPError as e:
            print(f"Error uploading PDFs: {e}")
            return []


def generate_output_path(dms_download_response: List[httpx.Response]) -> str:
    """Generate output path for merged PDF based on DMS codes."""
    try:
        first_pdf_name = dms_download_response[0].headers['content-disposition'].split(
            ';')[1].strip().split('=')[1].split('.')[0]
        last_pdf_name = dms_download_response[-1].headers['content-disposition'].split(
            ';')[1].strip().split('=')[1].split('.')[0]
        pdf_name = f"{first_pdf_name}_{last_pdf_name}_merged"
        return f"downloads/{pdf_name}.pdf"
    except (KeyError, IndexError) as e:
        print(f"Error while generating result PDF name: {e}")
        return f"downloads/merged_pdf_{uuid.uuid4()}.pdf"


def merge_pdfs(pdf_dms_order: List[str], ) -> None:
    """
    Merges multiple PDFs from DMS codes into a single output PDF.

    Args:
        pdf_dms_order (List[str]): A list of DMS codes for the PDFs to be merged.
        output_path (str, optional): Path to the output PDF file. Defaults to "final_pdf.pdf".

    Raises:
        IOError: If an error occurs during file reading or writing.
    """

    dms_download_response = asyncio.run(download_pdf(pdf_dms_order))

    upload_pdf_dms = []

    if dms_download_response:
        output_path = generate_output_path(dms_download_response)
        merged_document = fitz.open()  # Create a new empty PDF document
        try:
            for response in dms_download_response:
                if response.status_code == 200:

                    with fitz.open(stream=response.content, filetype="pdf") as pdf_file:
                        merged_document.insert_pdf(pdf_file)

        except IOError as e:
            print(f"Error merging PDFs: {e}")
            merged_document.close()  # Ensure cleanup
            # raise  # Re-raise the exception for proper handling

        # Save the merged document
        merged_document.save(output_path)
        merged_document.close()  # Close the merged document

        dms_upload_response = asyncio.run(upload_pdf([output_path]))

        if dms_upload_response:
            upload_pdf_dms.append(
                dms_upload_response[0].json().get("documentId"))

        print(f"Merged {len(pdf_dms_order)} PDFs into {output_path}")

    return upload_pdf_dms

# Example usage:
# pdf_dms_order = ["65d0e0ff324f5f8cc89feaa5", "65d141f5a60a5239c2e528e1"]
# import time
# s = time.time()
# merge_pdfs(pdf_dms_order)
# print(time.time() - s)
