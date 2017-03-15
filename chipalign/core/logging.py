import logging


class LoggerWithExtras(logging.LoggerAdapter):
    """
    Logging adapter that updates the extra values passed to the message with aditional
    values that are specific to the instance.

    Note that in case of a name collision, the extra parameters in the message
     will be overriden silently.
    """

    def process(self, msg, kwargs):

        # Update the extra parameters in the message with self.extra
        try:
            # Note that this silently overrides the extra parameters if there's a collision
            kwargs["extra"].update(self.extra)
        except KeyError:
            kwargs["extra"] = self.extra

        return msg, kwargs
