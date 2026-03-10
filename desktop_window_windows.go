//go:build cgo && windows

package main

import (
	"fmt"
	"sync"
	"unsafe"

	webview "github.com/webview/webview_go"
	"golang.org/x/sys/windows"
)

var (
	kernel32DLL                   = windows.NewLazySystemDLL("kernel32.dll")
	kernel32ProcGetModuleHandle   = kernel32DLL.NewProc("GetModuleHandleW")
	ole32DLL                      = windows.NewLazySystemDLL("ole32.dll")
	ole32ProcCoInitializeEx       = ole32DLL.NewProc("CoInitializeEx")
	user32DLL                     = windows.NewLazySystemDLL("user32.dll")
	user32ProcCreateWindowEx      = user32DLL.NewProc("CreateWindowExW")
	user32ProcDefWindowProc       = user32DLL.NewProc("DefWindowProcW")
	user32ProcDestroyWindow       = user32DLL.NewProc("DestroyWindow")
	user32ProcFindWindowEx        = user32DLL.NewProc("FindWindowExW")
	user32ProcLoadIcon            = user32DLL.NewProc("LoadIconW")
	user32ProcGetClientRect       = user32DLL.NewProc("GetClientRect")
	user32ProcGetSystemMetrics    = user32DLL.NewProc("GetSystemMetrics")
	user32ProcIsWindow            = user32DLL.NewProc("IsWindow")
	user32ProcMoveWindow          = user32DLL.NewProc("MoveWindow")
	user32ProcPostMessage         = user32DLL.NewProc("PostMessageW")
	user32ProcPostQuitMessage     = user32DLL.NewProc("PostQuitMessage")
	user32ProcRegisterClassEx     = user32DLL.NewProc("RegisterClassExW")
	user32ProcSetForegroundWindow = user32DLL.NewProc("SetForegroundWindow")
	user32ProcShowWindow          = user32DLL.NewProc("ShowWindow")
	user32ProcUpdateWindow        = user32DLL.NewProc("UpdateWindow")
	comInitOnce                   sync.Once
	comInitErr                    error
	hostWindowClassOnce           sync.Once
	hostWindowClassErr            error
	hostWindowProcPtr             = windows.NewCallback(hostWindowProc)
)

const (
	wmSize                  = 0x0005
	wmClose                 = 0x0010
	wmDestroy               = 0x0002
	wsPopup                 = 0x80000000
	wsClipChildren          = 0x02000000
	wsClipSiblings          = 0x04000000
	wsExAppWindow           = 0x00040000
	swHide                  = 0
	swShow                  = 5
	swMinimize              = 6
	smCxScreen              = 0
	smCyScreen              = 1
	coInitApartmentThreaded = 0x2
	sFalse                  = 0x1
)

type hostRect struct {
	Left   int32
	Top    int32
	Right  int32
	Bottom int32
}

type hostWndClassEx struct {
	CbSize        uint32
	Style         uint32
	LpfnWndProc   uintptr
	CbClsExtra    int32
	CbWndExtra    int32
	HInstance     windows.Handle
	HIcon         windows.Handle
	HCursor       windows.Handle
	HbrBackground windows.Handle
	LpszMenuName  *uint16
	LpszClassName *uint16
	HIconSm       windows.Handle
}

func newDesktopWebView(debug bool) (webview.WebView, error) {
	if err := ensureDesktopCOMInitialized(); err != nil {
		return nil, err
	}
	hostWindow, err := createHiddenDesktopHostWindow()
	if err != nil {
		return nil, err
	}
	return webview.NewWindow(debug, hostWindow), nil
}

func destroyDesktopWindow(w webview.WebView) {
	hwnd := desktopWindowHandle(w)
	if hwnd == 0 || !isWindow(hwnd) {
		return
	}
	_, _, _ = user32ProcDestroyWindow.Call(hwnd)
}

func applyDesktopWindowFrame(w webview.WebView) {
	resizeDesktopWidget(desktopWindowHandle(w))
}

func showDesktopWindow(w webview.WebView) error {
	hwnd := desktopWindowHandle(w)
	if hwnd == 0 {
		return nil
	}
	resizeDesktopWidget(hwnd)
	_, _, _ = user32ProcShowWindow.Call(hwnd, swShow)
	_, _, _ = user32ProcUpdateWindow.Call(hwnd)
	_, _, _ = user32ProcSetForegroundWindow.Call(hwnd)
	return nil
}

func minimizeDesktopWindow(w webview.WebView) error {
	hwnd := desktopWindowHandle(w)
	if hwnd == 0 {
		return nil
	}
	minimizeDesktopWindowHandle(hwnd)
	return nil
}

func minimizeDesktopWindowHandle(hwnd uintptr) {
	if hwnd == 0 {
		return
	}
	_, _, _ = user32ProcShowWindow.Call(hwnd, swMinimize)
}

func closeDesktopWindow(w webview.WebView) error {
	hwnd := desktopWindowHandle(w)
	if hwnd == 0 {
		return nil
	}
	_, _, _ = user32ProcPostMessage.Call(hwnd, wmClose, 0, 0)
	return nil
}

func createHiddenDesktopHostWindow() (unsafe.Pointer, error) {
	if err := ensureHostWindowClass(); err != nil {
		return nil, err
	}

	hInstance, err := getModuleHandle()
	if err != nil {
		return nil, fmt.Errorf("get module handle: %w", err)
	}

	screenW := getSystemMetric(smCxScreen)
	screenH := getSystemMetric(smCyScreen)
	hwnd, _, callErr := user32ProcCreateWindowEx.Call(
		wsExAppWindow,
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr("PinPulseDesktopHost"))),
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr("PinPulse"))),
		wsPopup|wsClipChildren|wsClipSiblings,
		0,
		0,
		uintptr(screenW),
		uintptr(screenH),
		0,
		0,
		uintptr(hInstance),
		0,
	)
	if hwnd == 0 {
		return nil, fmt.Errorf("create desktop host window: %w", callErr)
	}
	_, _, _ = user32ProcShowWindow.Call(hwnd, swHide)
	return unsafe.Pointer(hwnd), nil
}

func ensureHostWindowClass() error {
	hostWindowClassOnce.Do(func() {
		hInstance, err := getModuleHandle()
		if err != nil {
			hostWindowClassErr = fmt.Errorf("get module handle: %w", err)
			return
		}
		appIcon := loadApplicationIcon(hInstance)

		wc := hostWndClassEx{
			CbSize:        uint32(unsafe.Sizeof(hostWndClassEx{})),
			LpfnWndProc:   hostWindowProcPtr,
			HInstance:     hInstance,
			HIcon:         appIcon,
			LpszClassName: windows.StringToUTF16Ptr("PinPulseDesktopHost"),
			HIconSm:       appIcon,
		}
		atom, _, callErr := user32ProcRegisterClassEx.Call(uintptr(unsafe.Pointer(&wc)))
		if atom == 0 {
			hostWindowClassErr = fmt.Errorf("register host window class: %w", callErr)
		}
	})
	return hostWindowClassErr
}

func hostWindowProc(hwnd uintptr, msg uint32, wparam, lparam uintptr) uintptr {
	switch msg {
	case wmSize:
		resizeDesktopWidget(hwnd)
		return 0
	case wmClose:
		_, _, _ = user32ProcDestroyWindow.Call(hwnd)
		return 0
	case wmDestroy:
		_, _, _ = user32ProcPostQuitMessage.Call(0)
		return 0
	default:
		ret, _, _ := user32ProcDefWindowProc.Call(hwnd, uintptr(msg), wparam, lparam)
		return ret
	}
}

func resizeDesktopWidget(hwnd uintptr) {
	if hwnd == 0 || !isWindow(hwnd) {
		return
	}

	child, _, _ := user32ProcFindWindowEx.Call(
		hwnd,
		0,
		uintptr(unsafe.Pointer(windows.StringToUTF16Ptr("webview_widget"))),
		0,
	)
	if child == 0 {
		return
	}

	var rect hostRect
	ok, _, _ := user32ProcGetClientRect.Call(hwnd, uintptr(unsafe.Pointer(&rect)))
	if ok == 0 {
		return
	}

	width := rect.Right - rect.Left
	height := rect.Bottom - rect.Top
	_, _, _ = user32ProcMoveWindow.Call(child, 0, 0, uintptr(width), uintptr(height), 1)
}

func desktopWindowHandle(w webview.WebView) uintptr {
	if w == nil {
		return 0
	}
	return uintptr(unsafe.Pointer(w.Window()))
}

func isWindow(hwnd uintptr) bool {
	ok, _, _ := user32ProcIsWindow.Call(hwnd)
	return ok != 0
}

func getSystemMetric(index uintptr) int {
	value, _, _ := user32ProcGetSystemMetrics.Call(index)
	return int(value)
}

func getModuleHandle() (windows.Handle, error) {
	handle, _, err := kernel32ProcGetModuleHandle.Call(0)
	if handle == 0 {
		return 0, err
	}
	return windows.Handle(handle), nil
}

func loadApplicationIcon(hInstance windows.Handle) windows.Handle {
	icon, _, _ := user32ProcLoadIcon.Call(uintptr(hInstance), 1)
	return windows.Handle(icon)
}

func ensureDesktopCOMInitialized() error {
	comInitOnce.Do(func() {
		hr, _, _ := ole32ProcCoInitializeEx.Call(0, coInitApartmentThreaded)
		if hr != 0 && hr != sFalse {
			comInitErr = fmt.Errorf("CoInitializeEx failed: 0x%x", hr)
		}
	})
	return comInitErr
}
