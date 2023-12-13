from pyfaktory import Job


class TestJobConstructor:
    def test_jid_setting(self):
        some_jid = "4f1f3f8b5f3d490bad67f06496ef3d00"

        job_1 = Job(jid=some_jid, jobtype="foo1", args=(5, 4))
        job_2 = Job(jobtype="foo2", args=(5, 4))
        job_3 = Job(jobtype="foo3", args=(5, 4))

        assert job_1.jid == some_jid
        assert job_2.jid != job_3.jid
