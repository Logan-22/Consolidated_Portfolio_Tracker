import { create_notification } from './create_notification.js'
import { change_title_color_in_chart } from './scripts.js';

const change_theme_button = document.getElementById('change-theme');

// Load theme from localStorage
const saved_theme = localStorage.getItem('theme');
if (saved_theme) {
  document.documentElement.classList.add(saved_theme);
  if(change_theme_button){
  change_theme_button.value = saved_theme.replaceAll("_"," ")
  }
}

if (change_theme_button) {
change_theme_button.addEventListener('change', () => {
  const theme_value = change_theme_button.value.replaceAll(" ", "_")
  if(theme_value == 'Dark_Theme'){
  document.documentElement.classList = ""
  document.documentElement.classList.add('preload-dark')
  } else if(theme_value == 'Zerodha_Theme'){
  document.documentElement.classList = ""
  document.documentElement.classList.add('preload-zerodha')
  } else{
    document.documentElement.classList = ""
  }

  create_notification(`Changed theme to ${change_theme_button.value}`, 'success')
  change_title_color_in_chart()
  localStorage.setItem('theme', theme_value);
  });
}
