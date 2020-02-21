"""Menu item that is used to change the current setting for an enum value."""
import subprocess

from bullet import Bullet, Input, Numbers, SlidePrompt, colors

from vigorish.cli.menu_item import MenuItem
from vigorish.config import Config, ConfigFile
from vigorish.constants import EMOJI_DICT, MENU_NUMBERS
from vigorish.enums import ConfigDataType, DataSet
from vigorish.util.result import Result
from vigorish.util.sys_helpers import validate_folder_path

NUMERIC_SETTING_UNITS = {
    "URL_SCRAPE_DELAY": "seconds",
    "BATCH_JOB_SETTINGS": "urls",
    "BATCH_SCRAPE_DELAY": "minutes",
}


class ChangeSetttingMenuItem(MenuItem):
    def __init__(self, setting_name: str, config_file: ConfigFile) -> None:
        self.menu_item_text = "Change Setting"
        self.menu_item_emoji = EMOJI_DICT.get("SPIRAL", "")
        self.pointer = EMOJI_DICT.get("HAND_POINTER", "")
        self.config_file = config_file
        setting = self.config_file.all_settings.get(setting_name)
        self.setting_name_title = setting.setting_name_title
        self.setting_name = setting.setting_name
        self.data_type = setting.data_type
        self.possible_values = setting.possible_values
        self.exit_menu = True

    @property
    def yes_no_dict(self):
        return {
            f"{MENU_NUMBERS.get(1)}  YES": True,
            f"{MENU_NUMBERS.get(2)}  NO": False,
        }

    @property
    def enum_dict(self):
        return {
            f"{MENU_NUMBERS.get(choice.value)}  {choice.name}": choice
            for choice in self.possible_values
        }

    def launch(self) -> Result:
        subprocess.run(["clear"])
        use_same_setting_menu = self.__get_yes_no_menu("Use same setting for all data sets?")
        selected_menu_item_text = use_same_setting_menu.launch()
        same_setting_for_all_data_sets = self.yes_no_dict.get(selected_menu_item_text)
        if same_setting_for_all_data_sets:
            data_sets = [DataSet.ALL]
        else:
            data_sets = [ds for ds in DataSet if ds != DataSet.ALL]
        user_selections = self.__get_new_setting(data_sets)
        updated_settings = self.__get_updated_settings(user_selections)
        for updated_setting in updated_settings:
            data_set, new_value = updated_setting
            result = self.config_file.change_setting(self.setting_name, data_set, new_value)
            if result.failure:
                return result
        return Result.Ok(self.exit_menu)

    def __get_yes_no_menu(self, prompt):
        return Bullet(
            prompt,
            choices=[choice for choice in self.yes_no_dict.keys()],
            bullet=f" {self.pointer}",
            margin=2,
            bullet_color=colors.bright(colors.foreground["cyan"]),
            background_color=colors.foreground["default"],
            background_on_switch=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.foreground["default"],
        )

    def __get_new_setting(self, data_sets):
        if self.data_type == ConfigDataType.ENUM or self.data_type == ConfigDataType.STRING:
            setting_prompts = SlidePrompt(self.__get_menu_prompts(data_sets))
            return setting_prompts.launch()
        return [self.__get_numeric_menu(data_set) for data_set in data_sets]

    def __get_menu_prompts(self, data_sets):
        if self.data_type == ConfigDataType.ENUM:
            return [self.__get_enum_menu(data_set) for data_set in data_sets]
        if self.data_type == ConfigDataType.STRING:
            return [self.__get_str_menu(data_set) for data_set in data_sets]
        return None

    def __get_enum_menu(self, data_set):
        return Bullet(
            f"Select a value for {self.setting_name_title} (Data Set = {data_set.name}): ",
            choices=[choice for choice in self.enum_dict.keys()],
            bullet=f" {self.pointer}",
            margin=2,
            bullet_color=colors.bright(colors.foreground["cyan"]),
            background_color=colors.foreground["default"],
            background_on_switch=colors.foreground["default"],
            word_color=colors.foreground["default"],
            word_on_switch=colors.foreground["default"],
        )

    def __get_str_menu(self, data_set):
        return Input(
            f"Enter a value for {self.setting_name_title} (Data Set = {data_set.name}): ",
            word_color=colors.foreground["default"],
        )

    def __get_numeric_menu(self, data_set):
        prompt = f"Enable {self.setting_name_title} (Data Set = {data_set.name})? "
        enable_setting_menu = self.__get_yes_no_menu(prompt)
        selected_menu_item_text = enable_setting_menu.launch()
        is_required = self.yes_no_dict.get(selected_menu_item_text)
        if not is_required:
            return (prompt, (is_required, None, None, None, None))
        prompt = f"Use random values (Data Set = {data_set.name})? "
        random_values_menu = self.__get_yes_no_menu(prompt)
        selected_menu_item_text = random_values_menu.launch()
        is_random = self.yes_no_dict.get(selected_menu_item_text)
        setting_units = NUMERIC_SETTING_UNITS.get(self.setting_name)
        if is_random:
            prompt_min = f"Enter the minimum value (in {setting_units}) for {self.setting_name_title} (Data Set = {data_set.name}): "
            prompt_max = f"Enter the maximum value (in {setting_units}) for {self.setting_name_title} (Data Set = {data_set.name}): "
            random_min_prompt = Numbers(prompt_min, word_color=colors.foreground["default"])
            random_max_prompt = Numbers(prompt_max, word_color=colors.foreground["default"])
            random_min = random_min_prompt.launch()
            random_max = random_max_prompt.launch()
            return (prompt, (is_required, is_random, None, random_min, random_max))
        else:
            prompt = f"Enter the value (in {setting_units}) for {self.setting_name_title} (Data Set = {data_set.name}): "
            new_value_prompt = Numbers(prompt, word_color=colors.foreground["default"])
            new_value = new_value_prompt.launch()
            return (prompt, (is_required, is_random, new_value, None, None))

    def __get_updated_settings(self, prompt_results):
        if self.data_type == ConfigDataType.ENUM:
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
