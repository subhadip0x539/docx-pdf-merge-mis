from concurrent import futures
import logging

import grpc
from grpc_generated_file import merge_pb2, merge_pb2_grpc
from service.merge import merge_pdfs
import os


class MergeServicer(merge_pb2_grpc.mergeServicer):
    def Merge(self, request, context):
        print("################")
        print(request.doc_ids)
        print(request.process_tag)
        print(request.config.doc_order)
        print(request.config.rotation)
        print("################")
        upload_pdf_dms = merge_pdfs(pdf_dms_order= request.config.doc_order)
        print("-->",upload_pdf_dms)
        response = merge_pb2.PDFMergeResponse()
        response.doc_ids.extend(upload_pdf_dms)

        return response


def serve():
    os.makedirs("downloads", exist_ok=True)
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    merge_pb2_grpc.add_mergeServicer_to_server(MergeServicer(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()