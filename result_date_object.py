class ResultDate:
    def __init__(self, row: dict):
        self.security_code = row.get('security_code')
        self.system_readable_date = row.get('system_readable_date')

