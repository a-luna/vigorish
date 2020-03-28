"""Menu item that is used to change the current setting for an enum value."""
import subprocess

from bullet import Bullet, Input, Numbers, SlidePrompt, colors

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import prompt_user_yes_no_cancel, prompt_user_yes_no
from vigorish.config.types import ConfigFile
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import ConfigType, DataSet
from vigorish.util.result import Result


NUMERIC_SETTING_UNITS = {
    "URL_SCRAPE_DELAY": "seconds",
    "BATCH_JOB_SETTINGS": "urls",
    "BATCH_SCRAPE_DELAY": "minutes",
}


class ChangeSetttingMenuItem(MenuItem):
    def __init__(self, setting_name: str, config: ConfigFile) -> None:
        self.menu_item_text = "Change Setting"
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL", "")
        self.pointer = EMOJI_DICT.get("HAND_POINTER", "")
        self.config = config
        setting = self.config.all_settings.get(setting_name)
        self.setting_name_title = setting.setting_name_title
        self.setting_name = setting.setting_name
        self.data_type = setting.data_type
        self.possible_values = setting.possible_values
        self.exit_menu = True

    @property
    def enum_dict(self):
        return {
            f"{MENU_NUMBERS.get(choice.value)}  {choice.name}": choice
            for choice in self.possible_values
        }

    @property
    def setting_units(self):
        return NUMERIC_SETTING_UNITS.get(self.setting_name, "")

    def launch(self) -> Result:
        subprocess.run(["clear"])
        result = prompt_user_yes_no_cancel("Use same setting for all data sets?")
        if result.failure:
            return Result.Ok(self.exit_menu)
        use_same_seting = result.value
        if use_same_seting:
            data_sets = [DataSet.ALL]
        else:
            data_sets = [ds for ds in DataSet if ds != DataSet.ALL]

        subprocess.run(["clear"])
        user_selections = self.__get_new_setting(data_sets)
        updated_settings = self.__get_updated_settings(user_selections)
        for data_set, new_value in updated_settings:
            result = self.config.change_setting(self.setting_name, data_set, new_value)
            if result.failure:
                return result
        return Result.Ok(self.exit_menu)

    def __get_new_setting(self, data_sets):
        if self.data_type == ConfigType.NUMERIC:
            return [self.__get_numeric_menu(data_set) for data_set in data_sets]
        setting_prompts = SlidePrompt(self.__get_menu_prompts(data_sets))
        return setting_prompts.launch()

    def __get_menu_prompts(self, data_sets):
        if self.data_type == ConfigType.ENUM:
            return [self.__get_enum_menu(data_set) for data_set in data_sets]
        return [self.__get_str_menu(data_set) for data_set in data_sets]

    def __get_enum_menu(self, data_set):
        return Bullet(
            f"Select a value for {self.setting_name_title} (Data Set = {data_set.name}): ",
            choices=[choice for choice in self.enum_dict.keys()],
            bullet="",
            shift=1,
            indent=2,
            margin=2,
            bullet_color=colors.foreground["default"],
            background_color=colors.foreground["default"],
            background_on_switch=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.bright(colors.foreground["cyan"]),
        )

    def __get_str_menu(self, data_set):
        return Input(
            f"Enter a value for {self.setting_name_title} (Data Set = {data_set.name}): ",
            word_color=colors.foreground["default"],
        )

    def __get_numeric_menu(self, data_set):
        prompt = f"Enable {self.setting_name_title} (Data Set = {data_set.name})? "
        result = prompt_user_yes_no(prompt)
        is_enabled = result.value
        if not is_enabled:
            return (prompt, (is_enabled, None, None, None, None))
        prompt = f"Use random values (Data Set = {data_set.name})? "
        result = prompt_user_yes_no(prompt)
        is_random = result.value
        if is_random:
            prompt_min = (
                f"Enter the minimum value (in {self.setting_units}) for {self.setting_name_title} "
                f"(Data Set = {data_set.name}): "
            )
            prompt_max = (
                f"Enter the maximum value (in {self.setting_units}) for {self.setting_name_title} "
                f"(Data Set = {data_set.name}): "
            )
            random_min_prompt = Numbers(prompt_min, word_color=colors.foreground["default"])
            random_max_prompt = Numbers(prompt_max, word_color=colors.foreground["default"])
            random_min = random_min_prompt.launch()
            random_max = random_max_prompt.launch()
            return (prompt, (is_enabled, is_random, None, int(random_min), int(random_max)))
        else:
            prompt = f"Enter the value (in {self.setting_units}) for {self.setting_name_title} (Data Set = {data_set.name}): "
            new_value_prompt = Numbers(prompt, word_color=colors.foreground["default"])
            new_value = new_value_prompt.launch()
            return (prompt, (is_enabled, is_random, int(new_value), None, None))

    def __get_updated_settings(self, prompt_results):
        if self.data_type == ConfigType.ENUM:
            return self.__get_updated_settings_enum(prompt_results)
        return self.__get_updated_settings_str_num(prompt_results)

    def __get_updated_settings_enum(self, prompt_results):
        updated_settings = []
        for result in prompt_results:
            prompt, selected_menu_item_text = result
            for data_set in DataSet:
                if data_set.name in prompt:
                    new_value = self.enum_dict.get(selected_menu_item_text)
                    updated_settings.append((data_set, new_value))
                    break
        return updated_settings

    def __get_updated_settings_str_num(self, prompt_results):
        updated_settings = []
        for result in prompt_results:
            prompt, new_value = result
            for data_set in DataSet:
                if data_set.name in prompt:
                    updated_settings.append((data_set, new_value))
                    break
        return updated_settings
