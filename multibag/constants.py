"""
Common data about the multibag profile.
"""
CURRENT_VERSION = "0.4"
CURRENT_REFERENCE = "https://github.com/usnistgov/multibag-py/blob/master/docs/multibag-profile-spec.md"

DEFAULT_TAG_DIR = "multibag"

class Version(object):
    """
    a version class that can facilitate comparisons
    """

    def _toint(self, field):
        try:
            return int(field)
        except ValueError:
            return field

    def __init__(self, vers):
        """
        convert a version string to a Version instance
        """
        self._vs = vers
        self.fields = self._vs.split('.')

    def __str__(self):
        return self._vs

    def __eq__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self.fields == other.fields

    def __lt__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self.fields < other.fields

    def __le__(self, other):
        if not isinstance(other, Version):
            other = Version(other)
        return self < other or self == other

    def __ge__(self, other):
        return not (self < other)
    def __gt__(self, other):
        return not self.__le__(other)
    def __ne__(self, other):
        return not (self == other)

