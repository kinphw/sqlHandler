import tkinter as tk
from tkinter import scrolledtext
import os


class ConnectionView:
    def __init__(self, notebook, app):
        self._frame = tk.Frame(notebook)
        self._app = app
        self._build_ui()

    def get_tab_frame(self):
        return self._frame

    def _build_ui(self):
        paned = tk.PanedWindow(self._frame, orient=tk.HORIZONTAL, sashwidth=5)
        paned.pack(fill="both", expand=True)

        left = tk.Frame(paned, width=380, bg="#f5f5f5")
        left.pack_propagate(False)
        paned.add(left, minsize=320)

        right = tk.Frame(paned)
        paned.add(right, minsize=300)

        self._build_form(left)
        self._build_log(right)

    # ------------------------------------------------------------------
    # Left: connection form
    # ------------------------------------------------------------------
    def _build_form(self, parent):
        pad = {"padx": 16, "pady": 6}

        tk.Label(
            parent, text="MySQL 연결 설정",
            font=("Arial", 12, "bold"), bg="#f5f5f5"
        ).pack(anchor="w", padx=16, pady=(16, 4))

        # .env load buttons
        env_frame = tk.Frame(parent, bg="#f5f5f5")
        env_frame.pack(fill="x", padx=16, pady=(0, 10))

        tk.Label(env_frame, text=".env 불러오기:", fg="gray", bg="#f5f5f5").pack(side="left")

        self.btn_load_dev = tk.Button(
            env_frame, text="Dev 기본값",
            relief="flat", bg="#e0e0e0", cursor="hand2", padx=8
        )
        self.btn_load_dev.pack(side="left", padx=(8, 4))

        self.btn_load_prod = tk.Button(
            env_frame, text="Prod 기본값",
            relief="flat", bg="#ffe0b2", cursor="hand2", padx=8
        )
        self.btn_load_prod.pack(side="left")

        # Form fields
        form = tk.LabelFrame(parent, text="연결 정보", padx=12, pady=10, bg="#f5f5f5")
        form.pack(fill="x", padx=16, pady=(0, 8))

        fields = [
            ("Host",     "entry_host",     30),
            ("Port",     "entry_port",     8),
            ("User",     "entry_user",     28),
            ("DB 이름",  "entry_db",       28),
        ]
        for row, (label, attr, width) in enumerate(fields):
            tk.Label(form, text=f"{label}:", width=9, anchor="e", bg="#f5f5f5").grid(
                row=row, column=0, sticky="e", pady=4
            )
            entry = tk.Entry(form, width=width)
            entry.grid(row=row, column=1, sticky="w", padx=(6, 0), pady=4)
            setattr(self, attr, entry)

        # Password with show/hide toggle
        tk.Label(form, text="Password:", width=9, anchor="e", bg="#f5f5f5").grid(
            row=4, column=0, sticky="e", pady=4
        )
        pw_frame = tk.Frame(form, bg="#f5f5f5")
        pw_frame.grid(row=4, column=1, sticky="w", padx=(6, 0), pady=4)

        self.entry_password = tk.Entry(pw_frame, width=20, show="*")
        self.entry_password.pack(side="left")

        self._var_show_pw = tk.BooleanVar(value=False)
        tk.Checkbutton(
            pw_frame, text="보기", variable=self._var_show_pw,
            command=self._toggle_pw, bg="#f5f5f5"
        ).pack(side="left", padx=(6, 0))

        form.columnconfigure(1, weight=1)

        # SSH hint
        self.lbl_ssh = tk.Label(
            parent, text="", fg="gray", font=("Arial", 8), bg="#f5f5f5",
            wraplength=340, justify="left"
        )
        self.lbl_ssh.pack(anchor="w", padx=16, pady=(0, 6))

        # Connect / Release buttons
        btn_frame = tk.Frame(parent, bg="#f5f5f5")
        btn_frame.pack(fill="x", padx=16, pady=8)

        self.btn_connect = tk.Button(
            btn_frame, text="연결", width=14,
            bg="#4a90d9", fg="white", relief="flat",
            cursor="hand2", font=("Arial", 10)
        )
        self.btn_connect.pack(side="left")

        self.btn_release = tk.Button(
            btn_frame, text="연결 해제", width=12,
            relief="flat", cursor="hand2"
        )
        self.btn_release.pack(side="left", padx=(8, 0))

        # Status
        status_outer = tk.Frame(parent, bg="#f5f5f5")
        status_outer.pack(fill="x", padx=16)

        tk.Label(status_outer, text="상태:", bg="#f5f5f5").pack(side="left")
        self.lbl_status = tk.Label(
            status_outer, text="연결 없음", fg="gray", bg="#f5f5f5"
        )
        self.lbl_status.pack(side="left", padx=(6, 0))

    # ------------------------------------------------------------------
    # Right: log
    # ------------------------------------------------------------------
    def _build_log(self, parent):
        tk.Label(
            parent, text="연결 로그", font=("Arial", 10, "bold")
        ).pack(anchor="w", padx=8, pady=(12, 2))

        self.log_text = scrolledtext.ScrolledText(
            parent, state="disabled", font=("Consolas", 9),
            wrap="word", bg="#1e1e1e", fg="#d4d4d4"
        )
        self.log_text.pack(fill="both", expand=True, padx=8, pady=(0, 8))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _toggle_pw(self):
        self.entry_password.config(show="" if self._var_show_pw.get() else "*")

    # ------------------------------------------------------------------
    # Public interface (used by controller)
    # ------------------------------------------------------------------
    def log(self, msg: str):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", msg + "\n")
        self.log_text.configure(state="disabled")
        self.log_text.see("end")

    def schedule(self, callback):
        """Run callback on the main thread."""
        self.log_text.after(0, callback)

    def set_status(self, text: str, color: str = "gray"):
        self.lbl_status.config(text=text, fg=color)

    def set_ssh_label(self, text: str):
        self.lbl_ssh.config(text=text)

    def set_values(self, host="", port="", user="", password="", db_name=""):
        for entry, val in [
            (self.entry_host,     host),
            (self.entry_port,     str(port)),
            (self.entry_user,     user),
            (self.entry_password, password),
            (self.entry_db,       db_name),
        ]:
            entry.delete(0, "end")
            entry.insert(0, val)

    def get_values(self) -> dict:
        return {
            "host":     self.entry_host.get().strip(),
            "port":     self.entry_port.get().strip(),
            "user":     self.entry_user.get().strip(),
            "password": self.entry_password.get(),
            "db_name":  self.entry_db.get().strip(),
        }

    def bind_event(self, event_name: str, callback):
        mapping = {
            "connect":    self.btn_connect,
            "release":    self.btn_release,
            "load_dev":   self.btn_load_dev,
            "load_prod":  self.btn_load_prod,
        }
        if event_name in mapping:
            mapping[event_name].config(command=callback)
