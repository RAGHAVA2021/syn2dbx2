from bridge import synapse


class _DbapiError(Exception):
    def __init__(self, *args, sqlstate=None, native_error=None):
        super().__init__(*args)
        self.sqlstate = sqlstate
        self.native_error = native_error


def test_sql_error_info_understands_dbapi_attrs():
    assert synapse.sql_error_info(_DbapiError('x', sqlstate='HYT00', native_error=40501)) == (40501, 'HYT00')


def test_sql_error_info_understands_dbapi_args():
    assert synapse.sql_error_info(_DbapiError('42000', 'request failed (105019)')) == (105019, '42000')
