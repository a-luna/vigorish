"""Menu item that is used to change the current setting for an enum value."""
import subprocess

from bullet import Bullet, Input, Numbers, SlidePrompt, colors
from getch import pause

from vigorish.cli.menu_item import MenuItem
from vigorish.cli.util import prompt_user_yes_no_cancel, prompt_user_yes_no, print_message
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
        self.setting = self.config.all_settings.get(setting_name)
        self.setting_name_title = self.setting.setting_name_title
        self.setting_name = self.setting.setting_name
        self.data_type = self.setting.data_type
        self.possible_values = self.setting.possible_values
        self.exit_menu = True

    @property
    def enum_dict(self):
        return {
            f"{MENU_NUMBERS.get(num)}  {choice.name}": choice
            for num, choice in enumerate(self.possible_values, start=1)
        }

    @property
    def setting_units(self):
        return NUMERIC_SETTING_UNITS.get(self.setting_name, "")

    def launch(self) -> Result:
        subprocess.run(["clear"])
        data_sets = self.__get_data_sets_setting_will_apply_to()
        if not data_sets:
            return Result.Ok(self.exit_menu)
        setting_changed = False
        while not setting_changed:
            subprocess.run(["clear"])
            user_selections = self.__get_new_setting(data_sets)
            updated_settings = self.__get_updated_settings(user_selections)
            for data_set, new_value in updated_settings:
                result = self.config.change_setting(self.setting_name, data_set, new_value)
                if result.failure:
                    if "URL delay" in result.error:
                        print_message(result.error, fg="bright_red", bold=True)
                        pause(message="Press any key to continue...")
                        continue
                    return result
            setting_changed = True
        return Result.Ok(self.exit_menu)

    def __get_data_sets_setting_will_apply_to(self):
        if self.setting.same_value_for_all_data_sets_is_required:
            return [DataSet.ALL]
        result = prompt_user_yes_no_cancel("Use same setting for all data sets?")
        if result.failure:
            return None
        use_same_setting = result.value
        return [DataSet.ALL] if use_same_setting else [ds for ds in DataSet if ds != DataSet.ALL]

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
        if not self.setting.cannot_be_disabled:
            prompt = f"Enable {self.setting_name_title} (Data Set = {data_set.name})? "
            result = prompt_user_yes_no(prompt)
            is_enabled = result.value
            if not is_enabled:
                return (prompt, (is_enabled, None, None, None, None))
        prompt = f"Use random values (Data Set = {data_set.name})? "
        result = prompt_user_yes_no(prompt)
        is_random = result.value
        if is_random:
            min_max_are_valid = False
            while not min_max_are_valid:
                subprocess.run(["clear"])
                prompt_min = (
                    f"Enter the minimum value (in {self.setting_units}) for "
                    f"{self.setting_name_title} (Data Set = {data_set.name}): "
                )
                prompt_max = (
                    f"Enter the maximum value (in {self.setting_units}) for "
                    f"{self.setting_name_title} (Data Set = {data_set.name}): "
                )
                random_min_prompt = Numbers(prompt_min, word_color=colors.foreground["default"])
                random_max_prompt = Numbers(prompt_max, word_color=colors.foreground["default"])
                random_min = random_min_prompt.launch()
                random_max = random_max_prompt.launch()
                if random_max > random_min:
                    min_max_are_valid = True
                    continue
                error = (
                    f"Error: maximum value ({random_max}) must be greater than minimum "
                    f"value ({random_min})"
                )
                print_message(error, fg="bright_red", bold=True)
                pause(message="Press any key to continue...")
            return (prompt, (True, is_random, None, int(random_min), int(random_max)))
        else:
            prompt = (
                f"Enter the value (in {self.setting_units}) for {self.setting_name_title} "
                f"(Data Set = {data_set.name}): "
            )
            new_value_prompt = Numbers(prompt, word_color=colors.foreground["default"])
            new_value = new_value_prompt.launch()
            return (prompt, (True, is_random, int(new_value), None, None))

    def __get_updated_settings(self, prompt_results):
        if self.data_type == ConfigType.ENUM:
            return self.__get_updated_settings_enum(prompt_results)
        return self.__get_updated_settings_str_num(prompt_results)

    def __get_updated_settings_enum(self, prompt_results):
        updated_settings = []
        for prompt, selected_menu_item_text in prompt_results:
            for data_set in DataSet:
                if data_set.name in prompt:
                    new_value = self.enum_dict.get(selected_menu_item_text)
                    updated_settings.append((data_set, new_value))
                    break
        return updated_settings

    def __get_updated_settings_str_num(self, prompt_results):
        updated_settings = []
        for prompt, new_value in prompt_results:
            for data_set in DataSet:
                if data_set.name in prompt:
                    updated_settings.append((data_set, new_value))
                    break
        return updated_settings