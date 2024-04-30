import logging

import cv2
import numpy as np

from library.common.constants import CONTROLLER_HOST_ENV, CONTROLLER_PORT_ENV
from library.common.utils import getenv_with_default, pack_data
from srvs.combiner.rpc_api.controller_client_api_handlers import ControllerClient
from library.common.cdi_config_model import populate_config_from_parent_config, get_parent_config
from srvs.combiner.db.cache_ops import get_obj_from_cache, front_obj_of_cache_queue, add_obj_to_cache, \
    dequeue_obj_from_cache_queue, delete_obj_from_cache

controller_host = getenv_with_default(CONTROLLER_HOST_ENV, "0.0.0.0")
controller_port = getenv_with_default(CONTROLLER_PORT_ENV, "50000")

"""
Does the following steps:
    1: Get the currently processing stream from redis - top of the queue
    2: Get the capture object from redis with the stream_id
    3: clear the data of CDI
    4: get the next frame from the capture
    5: populate the CDI
    6: update frame_order on the cache object and add it to the cache
    7: Transfer the CDI
"""


def populate_and_transfer_cdis(config):
    logging.info("perform_cdi_ops: starting...")
    global controller_host, controller_port
    populate_config_from_parent_config(config=config)
    logging.info("perform_cdi_ops: fetching top stream obj from queue")
    submit_task_model = front_obj_of_cache_queue()
    if submit_task_model is None:
        logging.error("perform_cdi_ops: No task found in queue")
        return

    logging.info(f"perform_cdi_ops: processing stream_id: {submit_task_model.stream_id}")
    logging.info(f"perform_cdi_ops: fetching extractor cache for stream_id: {submit_task_model.stream_id}")
    extractor_cache_obj = get_obj_from_cache(stream_id=submit_task_model.stream_id)
    if extractor_cache_obj is None:
        raise Exception(f"perform_cdi_ops: No Extractor Cache found for stream_id: {submit_task_model.stream_id}")

    logging.info(f"perform_cdi_ops: populating CDIs for stream_id: {submit_task_model.stream_id}")
    to_send_cdis = {}

    for cdi_key, cdi in config.cdis:
        cdi.clear_data()
        ret, frame = extractor_cache_obj.player.read()
        logging.info(
            f"perform_cdi_ops: Extracted frame {extractor_cache_obj.frame_order + 1} out of {extractor_cache_obj.frame_count} for {submit_task_model.stream_id}")
        if not ret:
            frame = np.zeros(shape=(1, 1, 1), dtype='uint8')
            packed_data = pack_data(stream_id=submit_task_model.stream_id,
                                    frame_count=extractor_cache_obj.frame_count,
                                    frame_order=(extractor_cache_obj.frame_order + 1),
                                    x_shape=extractor_cache_obj.x_shape,
                                    y_shape=extractor_cache_obj.y_shape, done=True, frame=frame,
                                    remote_video_save_dir_path=submit_task_model.remote_video_save_dir_path,
                                    sftp_host=submit_task_model.sftp_host, sftp_port=submit_task_model.sftp_port,
                                    sftp_user=submit_task_model.sftp_user, sftp_pwd=submit_task_model.sftp_pwd)
            cdi.write_data(data=packed_data)
            to_send_cdis[cdi_key] = cdi
            dequeue_obj_from_cache_queue()
            delete_obj_from_cache(stream_id=submit_task_model.stream_id)
            logging.info(f"perform_cdi_ops: Done processing all frames for stream_id {submit_task_model.stream_id}")
            break
        packed_data = pack_data(stream_id=submit_task_model.stream_id,
                                frame_count=extractor_cache_obj.frame_count,
                                frame_order=(extractor_cache_obj.frame_order + 1),
                                x_shape=extractor_cache_obj.x_shape,
                                y_shape=extractor_cache_obj.y_shape, done=False, frame=frame,
                                remote_video_save_dir_path=submit_task_model.remote_video_save_dir_path,
                                sftp_host=submit_task_model.sftp_host, sftp_port=submit_task_model.sftp_port,
                                sftp_user=submit_task_model.sftp_user, sftp_pwd=submit_task_model.sftp_pwd)
        cdi.write_data(data=packed_data)
        to_send_cdis[cdi_key] = cdi
        extractor_cache_obj.frame_order += 1
        add_obj_to_cache(stream_id=submit_task_model.stream_id, obj=extractor_cache_obj)

    extractor_cache_obj.player.release()
    cv2.destroyAllWindows()
    config.cdis = to_send_cdis
    logging.info(
        f"perform_cdi_ops: Transferring CDI to process_id: {config.transfer_id} for stream_id: {submit_task_model.stream_id}")
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.TransferCDIs(config=config)
    if response.err != "":
        raise Exception(f"perform_cdi_ops: exception while transferring cdis: {response.err}")


def handle_cdi_create(controller_host, controller_port):
    parent_config = get_parent_config()
    if parent_config.create:
        controller_client = ControllerClient(host=controller_host, port=controller_port)
        response = controller_client.CreateCDIs(config=parent_config)
        if response.err != "":
            raise Exception(f"handle_cdi_create: exception while creating cdis: {response.err}")


def delete_cdis(controller_host, controller_port, process_id, config):
    controller_client = ControllerClient(host=controller_host, port=controller_port)
    response = controller_client.DeleteCDIs(process_id=process_id, config=config)
    if response.err != "":
        raise Exception(f"create_cdis: exception while deleting cdis: {response.err}")
