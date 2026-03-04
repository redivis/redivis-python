from ..common.TabularReader import TabularReader


class ReadStream(TabularReader):
    def __init__(self, id, *, table, query, upload, properties=None):
        super().__init__(is_read_stream=True)
        self.id = id
        self.table = table
        self.query = query
        self.upload = upload
        self.uri = f"/readStreams/{id}"
        self.properties = properties if properties is not None else {}

    def __repr__(self):
        return f"<ReadStream {self.id}>"
