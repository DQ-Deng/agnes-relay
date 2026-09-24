import sys
import subprocess

def check_interpreter_consistency():
    print(f"当前解释器路径: {sys.executable}")
    print(f"Python版本: {sys.version}")
    
    # 获取当前环境已安装包
    result = subprocess.run([sys.executable, "-m", "pip", "list"], 
                            capture_output=True, text=True)
    packages = [line.split()[0] for line in result.stdout.splitlines()[2:] if line.strip()]
    
    print(f"检测到 {len(packages)} 个已安装包")
    print("前5个包:", packages[:5])
    
    # 建议：将此脚本在PyCharm Terminal和系统Terminal分别运行对比输出

if __name__ == "__main__":
    check_interpreter_consistency()