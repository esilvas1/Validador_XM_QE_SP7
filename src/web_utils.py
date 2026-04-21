"""
Utilidades para adaptar funciones de tkinter al contexto web de Django
"""
import os

# Flag para indicar si estamos en contexto web
WEB_CONTEXT = os.environ.get('DJANGO_SETTINGS_MODULE') is not None


class WebMessageBox:
    """
    Reemplazo de tkinter.messagebox para contexto web
    En web, las confirmaciones se manejan en el frontend
    """
    
    @staticmethod
    def askyesno(title, message):
        """
        En contexto web, siempre retorna True ya que las confirmaciones
        se manejan en el frontend antes de llamar al endpoint
        """
        if WEB_CONTEXT:
            print(f"[{title}] {message}")
            return True
        else:
            # En contexto desktop, usar tkinter normal
            from tkinter import messagebox
            return messagebox.askyesno(title, message)
    
    @staticmethod
    def showinfo(title, message):
        """Muestra mensaje informativo"""
        if WEB_CONTEXT:
            print(f"ℹ️ [{title}] {message}")
        else:
            from tkinter import messagebox
            messagebox.showinfo(title, message)
    
    @staticmethod
    def showerror(title, message):
        """Muestra mensaje de error"""
        if WEB_CONTEXT:
            print(f"❌ [{title}] {message}")
        else:
            from tkinter import messagebox
            messagebox.showerror(title, message)


# Instancia global para usar en lugar de tkinter.messagebox
messagebox = WebMessageBox()
