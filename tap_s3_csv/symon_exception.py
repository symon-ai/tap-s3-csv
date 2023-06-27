class SymonException(Exception):
    def __init__(self, message, code, keywords=None):
        super().__init__(message)
        self.code = code
        self.keywords = keywords
