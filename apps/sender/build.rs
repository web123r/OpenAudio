fn main() {
    #[cfg(windows)]
    {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("icon.ico");
        res.compile()
            .expect("failed to embed icon.ico as a Windows resource");
    }
}
