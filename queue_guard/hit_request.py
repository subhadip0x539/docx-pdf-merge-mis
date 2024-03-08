import grpc
from grpc_generated_file import merge_pb2, merge_pb2_grpc

async def run(json_data):
    async with grpc.aio.insecure_channel("localhost:50051") as channel:
        stub = merge_pb2_grpc.mergeStub(channel)
        request = merge_pb2.PDFMergeRequest()
        request.doc_ids.extend(json_data["docIds"])

        request.process_tag = json_data["processTag"]

        config = merge_pb2.Config()
        config.doc_order.extend(json_data['config']['docOrder'])
        for doc_id, rot in json_data['config']['rotation'].items():
            config.rotation.add(doc_id=doc_id, rotation=rot)

        request.config.CopyFrom(config)

        response = await stub.Merge(request) 

    return response.doc_ids
