import logging
from flask import Flask, request, jsonify
from srvs.detector.detector import Object_Detector

app = Flask(__name__)

class ProcessService:
    def __init__(self, *args, **kwargs):
        pass

    def TransferPayload(self, payload):
        logging.info("Received payload.")
        object_detector_obj = Object_Detector(packed_data=payload)
        object_detector_obj.run()
        return {"err": ""}

@app.route("/transfer_payload", methods=["POST"])
def transfer_payload():
    payload = request.json["payload"]
    process_service = ProcessService()
    response = process_service.TransferPayload(payload)
    return jsonify(response)

def serve_rest(rest_host, rest_port):
    logging.info(f"Starting REST API server on : {rest_host}:{rest_port}")
    app.run(host=rest_host, port=rest_port)