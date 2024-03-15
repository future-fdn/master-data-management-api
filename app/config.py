from dynaconf import Dynaconf

settings = Dynaconf(
    settings_files=[".settings.toml", ".secrets.toml"],
    envvar_prefix="APP",
    env_switcher="APP_APP_ENV",
    dotenv_path=".env",
    load_dotenv=True,
    environments=True,
)
