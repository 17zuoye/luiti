#-*-coding:utf-8-*-

from etl_utils import cached_property
import inspect

class ExtUtils(object):

    class ExtendClass(object):
        """
        Extend a class dynamically, and compact with `property` and `cached_property` in a unified call mechanism.
        """

        @classmethod
        def extend(cls, attrs):
            for attr_k1, attr_v1  in attrs.iteritems():
                orig_attr = getattr(cls, attr_k1, None)

                # convert input to original value type
                if isinstance(orig_attr, property) and (not isinstance(attr_v1, property)):
                    new_v1 = property(attr_v1)
                elif isinstance(orig_attr, cached_property) and (not isinstance(attr_v1, cached_property)):
                    new_v1 = cached_property(attr_v1)
                else:
                    new_v1 = attr_v1

                setattr(cls, attr_k1, new_v1)
