class Submit_Task_Model:
    def __init__(self, stream_id="", remote_video_fetch_path="/sftpuser/dogs.mp4",
                 remote_video_save_dir_path=f"/sftpuser/", sftp_host="amd111.utah.cloudlab.us",
                 sftp_port=22, sftp_user="sftpuser", sftp_pwd="sftpuser"):
        self.stream_id = stream_id
        self.remote_video_fetch_path = remote_video_fetch_path
        self.remote_video_save_dir_path = remote_video_save_dir_path

        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_user = sftp_user
        self.sftp_pwd = sftp_pwd

    @classmethod
    def load_json(cls, request):
        if "stream_id" in request.keys():
            return cls(stream_id=request["stream_id"], remote_video_fetch_path=request["remote_video_fetch_path"],
                       remote_video_save_dir_path=request["remote_video_save_dir_path"], sftp_host=request["sftp_host"],
                       sftp_port=request["sftp_port"], sftp_user=request["sftp_user"], sftp_pwd=request["sftp_pwd"])

        return cls(remote_video_fetch_path=request["remote_video_fetch_path"],
                   remote_video_save_dir_path=request["remote_video_save_dir_path"], sftp_host=request["sftp_host"],
                   sftp_port=request["sftp_port"], sftp_user=request["sftp_user"], sftp_pwd=request["sftp_pwd"])

    def to_dict(self):
        return {"stream_id": self.stream_id,
                "remote_video_fetch_path": self.remote_video_fetch_path,
                "remote_video_save_dir_path": self.remote_video_save_dir_path,
                "sftp_host": self.sftp_host,
                "sftp_port": self.sftp_port,
                "sftp_user": self.sftp_user,
                "sftp_pwd": self.sftp_pwd}
