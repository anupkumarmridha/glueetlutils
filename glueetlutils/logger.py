import logging

def setup_logger(name="glueetlutils", level=logging.INFO):
    """
    Set up a global logger for the glueetlutils package.
    Ensures consistent logging format and level across all modules.

    :param name: Logger name, default is 'glueetlutils'
    :param level: Logging level, default is INFO
    :return: Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if not logger.hasHandlers():
        handler = logging.StreamHandler()  # Console handler
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger

# Create a global logger instance
logger = setup_logger()
