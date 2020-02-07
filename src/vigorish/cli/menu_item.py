class MenuItem:
    _menu_item_text: str = ""
    menu_item_emoji: str = ""
    exit_menu: bool = False

    @property
    def menu_item_text(self) -> str:
        return f"{self.menu_item_emoji}  {self._menu_item_text}  "

    @menu_item_text.setter
    def menu_item_text(self, menu_item_text: str) -> None:
        self._menu_item_text = menu_item_text
