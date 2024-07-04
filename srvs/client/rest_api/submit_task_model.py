class Submit_Task_Model:
    def __init__(self, data = "", size=0):
        self.data = data
        self.size = size

    @classmethod
    def load_json(cls, request):

        return cls(data = request["data"], size = int(request["size"]))

    def to_dict(self):
        return {"data": self.data,
                "size": self.size}
