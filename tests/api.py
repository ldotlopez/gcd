import unittest


import gcd


class API(gcd.API):
    def __init__(self, *args, **kwargs):
        # kwargs['storage_backend'] = 'sa'
        # kwargs['storage_opts'] = {
        #     'dburi': 'sqlite:///:memory:'
        # }
        kwargs['storage_backend'] = 'default'
        super().__init__(*args, **kwargs)


class TestAPI(unittest.TestCase):
    def test_save(self):
        api = API()
        api.save('foo', 1)

        res = api.get('foo')
        self.assertEqual(res, 1)

    def test_save_with_log(self):
        api = API()
        api.save('foo', 1)
        api.save('foo', 2)

        res = api.get('foo')
        self.assertEqual(res, 2)

        res = list(api.log('foo'))
        self.assertEqual(res, [2, 1])

    def test_get_tag_error(self):
        api = API()

        with self.assertRaises(gcd.TagError) as cm:
            api.get('foo')

    def test_log_tag_error(self):
        api = API()

        with self.assertRaises(gcd.TagError) as cm:
            next(api.log('foo'))

    def test_notify(self):
        called = False

        def callback(sender, value, prev):
            nonlocal called
            called = True

        api = API()
        api.connect(gcd.Event.VALUE_CHANGED, callback)
        api.save('foo', 1)

        self.assertTrue(called)


if __name__ == '__main__':
    unittest.main()
