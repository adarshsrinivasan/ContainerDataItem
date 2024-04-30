class CDI_Config:
    def __init__(self, cdi_id="", cdi_key=0, cdi_size_bytes=0):
        self.cdi_id = cdi_id
        self.cdi_key = cdi_key
        self.cdi_size_bytes = cdi_size_bytes


class CDI_OP:
    def __init__(self, cdi_id="", cdi_access_mode=666, destroy_if_no_new_data=True, create=True, op="TRANSFER",
                 transfer_id="", transfer_mode=666):
        self.cdi_id = cdi_id
        self.cdi_access_mode = cdi_access_mode
        self.destroy_if_no_new_data = destroy_if_no_new_data
        self.create = create
        self.op = op
        self.transfer_id = transfer_id
        self.transfer_mode = transfer_mode


class Process_Config:
    def __init__(self, app_id="", name="", id="", uid=0, gid=0, cdi_ops=None, cdi_configs=None):
        if cdi_ops is None:
            cdi_ops = []
        if cdi_configs is None:
            cdi_configs = []

        self.app_id = app_id
        self.name = name
        self.id = id
        self.uid = uid
        self.gid = gid
        self.cdi_ops = cdi_ops
        self.cdi_configs = cdi_configs

    @classmethod
    def load_dict(cls, id, uid, gid, process_config_dict):
        app_id = process_config_dict.get('app_id', "")
        cdi_configs_dict = process_config_dict.get('cdi_configs', [])
        process_configs_dict = process_config_dict.get('process_configs', [])
        my_config_dict = None

        for process_config_dict in process_configs_dict:
            if id == process_config_dict["id"]:
                my_config_dict = process_config_dict
                break
        if my_config_dict is None:
            raise Exception(f"Exception while parsing config. No config found for {name}")
        cdi_ops_dict = my_config_dict["cdi_ops"]

        cdi_ops = []
        cdi_configs = []

        for cdi_config_dict in cdi_configs_dict:
            cdi_configs.append(CDI_Config(
                cdi_id=cdi_config_dict["cdi_id"],
                cdi_key=cdi_config_dict["cdi_key"],
                cdi_size_bytes=cdi_config_dict["cdi_size_bytes"]
            ))

        for cdi_op_dict in cdi_ops_dict:
            cdi_ops.append(CDI_OP(
                cdi_id=cdi_op_dict["cdi_id"],
                cdi_access_mode=cdi_op_dict["cdi_access_mode"],
                destroy_if_no_new_data=cdi_op_dict["destroy_if_no_new_data"],
                create=cdi_op_dict["create"],
                op=cdi_op_dict["op"],
                transfer_id=cdi_op_dict["transfer_id"],
                transfer_mode=cdi_op_dict["transfer_mode"]
            ))

        return cls(
            app_id=app_id,
            name=name,
            id=my_config_dict["id"],
            uid=uid,
            gid=gid,
            cdi_ops=cdi_ops,
            cdi_configs=cdi_configs
        )
