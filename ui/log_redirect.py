import tkinter as tk


class TextRedirector:
    """Redirects stdout to a tkinter Text widget."""

    def __init__(self, widget: tk.Text):
        self.widget = widget

    def write(self, text: str):
        self.widget.configure(state='normal')
        self.widget.insert(tk.END, text)
        self.widget.see(tk.END)
        self.widget.configure(state='disabled')
        self.widget.update_idletasks()

    def flush(self):
        pass
