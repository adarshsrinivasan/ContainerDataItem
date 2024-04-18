class CDI_Access:
    def __init__(self, cdi_id, shm_key, shm_shmid):
        self.cdi_id = cdi_id
        self.shm_key = shm_key
        self.shm_shmid = shm_shmid


class Process_Config:

    def __init__(self, process_id="", process_node_ip="", cdi_access_list=None):
        if cdi_access_list is None:
            cdi_access_list = []
        self.process_id = process_id
        self.process_node_ip = process_node_ip
        self.cdi_access_list = cdi_access_list

    def to_dict(self):
        cdi_access_list_dicts = []
        for cdi_access in self.cdi_access_list:
            cdi_access_list_dicts.append({
                'cdi_id': cdi_access.cdi_id,
                'shm_key': cdi_access.shm_key,
                'shm_shmid': cdi_access.shm_shmid
            })
        return {
            'process_id': self.process_id,
            'process_node_ip': self.process_node_ip,
            'cdi_access_list': cdi_access_list_dicts
        }

    @classmethod
    def load_dict(cls, dict_data):
        cdi_access_list = []
        for cdi_access_dict in dict_data.get('cdi_access_list', []):
            cdi_access_list.append(CDI_Access(
                cdi_access_dict['cdi_id'],
                cdi_access_dict['shm_key'],
                cdi_access_dict['shm_shmid']
            ))
        return cls(
            process_id=dict_data.get('process_id', ''),
            process_node_ip=dict_data.get('process_node_ip', ''),
            cdi_access_list=cdi_access_list
        )
