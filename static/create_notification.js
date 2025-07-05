export function create_notification(message, type = 'info', duration = 4000) {
    const container = document.getElementById("notification-container");
    type = type.toLowerCase().replaceAll(" ","_")
    if (type != "success"){
        duration = 10000
    }

    const toast = document.createElement("div");
    toast.classList.add("toast", `toast-${type}`);

    // Icons based on type
    const iconMap = {
        success: "✅",
        error: "❌",
        failed: "❌",
        warning: "⚠️",
        info: "ℹ️",
        duplicate_issue: "👯‍♂️"
    };

    const icon = document.createElement("span");
    icon.classList.add("toast-icon");
    icon.textContent = iconMap[type] || "";

    const text = document.createElement("span");
    text.textContent = message;

    toast.appendChild(icon);
    toast.appendChild(text);
    container.appendChild(toast);

    // Auto-dismiss
    setTimeout(() => {
        toast.classList.add("hide");
        setTimeout(() => toast.remove(), 500); // remove after transition
    }, duration);
}