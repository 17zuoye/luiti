# -*-coding:utf-8-*-

from etl_utils import cached_property
from ..luigi_extensions import ArrowParameter


class VisualiserEnvTemplate(object):
    """
    Setup luiti webui.

    Overwrite below attributes, see keys and their examples in `data`.
    """
    def __init__(self, kwargs=dict()):
        assert isinstance(kwargs, dict), kwargs

        for k1, v1 in kwargs.iteritems():
            if not hasattr(self, k1):
                raise ValueError("%s dont has attribute \"%s\"" % self, k1)
            setattr(self, k1, v1)

    @cached_property
    def data(self):
        def maybe_call(o1):
            if callable(o1):
                o1 = o1()
            return o1

        result = {
            "file_web_url_prefix": maybe_call(self.file_web_url_prefix),
            "date_begin": maybe_call(self.date_begin),
            "additional_task_parameters": maybe_call(self.additional_task_parameters),
            "package_config": maybe_call(self.package_config),
        }

        # check data valid
        assert isinstance(result["additional_task_parameters"], dict)
        if len(result["additional_task_parameters"]) > 0:
            val = result["additional_task_parameters"].values()[0]
            assert "values" in val
            assert "default" in val

        return result

    def __getitem__(self, k1):
        return self.data[k1]

    # API list
    file_web_url_prefix = ""
    date_begin = ArrowParameter.now().replace(weeks=-1).format("YYYY-MM-DD")

    def additional_task_parameters(self):
        """
        Example is

        {
            "subject": {
                "values": ["english", "math"],
                "default": "english",
            }
        }
        """
        return dict()

    def package_config(self):
        return {
            "default_selected": []
        }
