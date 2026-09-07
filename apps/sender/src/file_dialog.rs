use std::path::PathBuf;
use rfd::FileDialog;

const EXTENSION: &str = "oapreset";
const DESCRIPTION: &str = "OpenAudio preset";

pub fn choose_open_file() -> Option<PathBuf> {
    FileDialog::new()
        .add_filter(DESCRIPTION, &[EXTENSION])
        .pick_file()
}

pub fn choose_save_file() -> Option<PathBuf> {
    FileDialog::new()
        .add_filter(DESCRIPTION, &[EXTENSION])
        .set_file_name("openaudio-preset.oapreset")
        .save_file()
}
