import tkinter as tk
from tkinter import messagebox
import threading
import sys
import io

def run_procesos():
    try:
        print("Inciando procesos... espere un momento")
        from processor import instalar_requisitos
        instalar_requisitos()

        print("Inciando conexión a Oracle")
        from conexion import open_conexion
        conn, engine = open_conexion()

        if conn:
            print("🟢 Conexión lista para consultas.")
            conn.close()
            engine.dispose()
            print("🔒 Conexión cerrada correctamente.")

        continuar = messagebox.askyesno("¿Continuar?", "¿Deseas continuar con la creación del CONSOLIDADO_QE?")
        if not continuar:
            print("🚫 Proceso cancelado por el usuario antes de crear CONSOLIDADO_QE")
            return

        from processor import crear_CONSOLIDADO_QE
        crear_CONSOLIDADO_QE()

        continuar = messagebox.askyesno("¿Continuar?", "¿Deseas continuar con la creación del CONSOLIDADO_SP7?")
        if not continuar:
            print("🚫 Proceso cancelado por el usuario antes de crear CONSOLIDADO_SP7")
            return

        from processor import crear_CONSOLIDADO_SP7
        crear_CONSOLIDADO_SP7()

        continuar = messagebox.askyesno("¿Continuar?", "¿Deseas continuar con la creación del CONSOLIDADO_XM?")
        if not continuar:
            print("🚫 Proceso cancelado por el usuario antes de crear CONSOLIDADO_XM")
            return

        from processor import crear_CONSOLIDADO_XM
        crear_CONSOLIDADO_XM()

        messagebox.showinfo("✔️ Finalizado", "Procesamiento terminado correctamente.")
    except Exception as e:
        messagebox.showerror("❌ Error", str(e))


def run_validation():
    print("Inciando procesos... espere un momento")
    try:
        continuar = messagebox.askyesno("¿Continuar?", "¿Deseas continuar con la validación del CONSOLIDADO_SP7?")
        if not continuar:
            print("🚫 Proceso cancelado por el usuario antes de validar CONSOLIDADO_SP7")
            return

        from validator import validar_CONSOLIDADO_SP7
        validar_CONSOLIDADO_SP7()

        continuar = messagebox.askyesno("¿Continuar?", "¿Deseas continuar con la validación del CONSOLIDADO_QE?")
        if not continuar:
            print("🚫 Proceso cancelado por el usuario antes de validar CONSOLIDADO_QE")
            return

        from validator import validar_CONSOLIDADO_QE
        validar_CONSOLIDADO_QE()

        continuar = messagebox.askyesno("¿Continuar?", "¿Deseas continuar con la validación del CONSOLIDADO_XM?")
        if not continuar:
            print("🚫 Proceso cancelado por el usuario antes de validar CONSOLIDADO_XM")
            return

        from validator import validar_CONSOLIDADO_XM
        validar_CONSOLIDADO_XM()

        messagebox.showinfo("✔️ Finalizado", "Validación terminada correctamente.")
    except Exception as e:
        messagebox.showerror("❌ Error", str(e))

def run_create_QA_TFDDREGISTRO():
    print("We are working about your requests")
    from crear_data_qa_tfddregistro import crear_QA_TFDDREGISTRO
    crear_QA_TFDDREGISTRO()


class GitBashLikeConsole(io.StringIO):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def write(self, message):
        self.text_widget.configure(state="normal")
        self.text_widget.insert(tk.END, message)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state="disabled")


def mostrar_splash(duracion=5):
    splash = tk.Tk()
    splash.overrideredirect(True)
    splash.configure(bg="black")

    screen_width = splash.winfo_screenwidth()
    screen_height = splash.winfo_screenheight()
    width = 600
    height = 200
    x = (screen_width - width) // 2
    y = (screen_height - height) // 2
    splash.geometry(f"{width}x{height}+{x}+{y}")

    label = tk.Label(
        splash,
            text="⚡ Programa de ajuste de eventos del SDL\n\nCentrales Eléctricas de N.S.",
        bg="black",
        fg="#00FF00",
        font=("Courier New", 13),
        justify="center"
    )
    label.pack(expand=True)

    splash.after(duracion * 1000, splash.destroy)
    splash.mainloop()


def start_app():
    ventana = tk.Tk()
    ventana.title("🔧 Validador general del OMS del SDL")

    screen_width = ventana.winfo_screenwidth()
    screen_height = ventana.winfo_screenheight()
    width = int(screen_width * 0.8)
    height = int(screen_height * 0.8)
    x = (screen_width - width) // 2
    y = (screen_height - height) // 2
    ventana.geometry(f"{width}x{height}+{x}+{y}")
    ventana.configure(bg="#4e423f")

    # Botones arriba
    boton_frame = tk.Frame(ventana, bg="#4e423f")
    boton_frame.pack(pady=10)

    ejecutar_btn = tk.Button(
        boton_frame,
        text="▶ Ejecutar Extracción",
        command=lambda: threading.Thread(target=run_procesos, daemon=True).start(),
        bg="#2c3e50",
        fg="white",
        font=("Arial", 11, "bold")
    )
    ejecutar_btn.pack(side="left", padx=10)

    validate_btn = tk.Button(
        boton_frame,
        text="▶ Validación de Datas",
        command=lambda: threading.Thread(target=run_validation, daemon=True).start(),
        bg="#2c3e50",
        fg="white",
        font=("Arial", 11, "bold")
    )
    validate_btn.pack(side="left", padx=10)

    generate_btn = tk.Button(
        boton_frame,
        text="▶ Generación QA_TFDDREGISTRO",
        command=lambda: threading.Thread(target=run_create_QA_TFDDREGISTRO, daemon=True).start(),
        bg="#2c3e50",
        fg="white",
        font=("Arial", 11, "bold")
    )
    generate_btn.pack(side="left", padx=10)

    salir_btn = tk.Button(
        boton_frame,
        text="❌ Salir",
        command=ventana.destroy,
        bg="darkred",
        fg="white",
        font=("Arial", 11, "bold")
    )
    salir_btn.pack(side="left", padx=10)

    # Consola debajo
    consola = tk.Text(
        ventana,
        bg="black",
        fg="#00FF00",
        insertbackground="white",
        font=("Courier New", 11),
        wrap=tk.WORD,
        state="disabled"
    )
    consola.pack(fill="both", expand=True, padx=10, pady=(0, 10))

    sys.stdout = GitBashLikeConsole(consola)
    sys.stderr = GitBashLikeConsole(consola)

    ventana.mainloop()


if __name__ == "__main__":
    mostrar_splash()
    start_app()
