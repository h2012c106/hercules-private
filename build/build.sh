#!/bin/bash

echo "Packing hercules..."

fatjarName=$1
targetDir=$2
tarName=$3

finalDirName="hercules"
randomDirName=$RANDOM
tmpDir="${targetDir}/${randomDirName}"
tmpHerculesDir="${tmpDir}/${finalDirName}"
fatjarPos="${targetDir}/${fatjarName}"

# 递归创建临时文件夹
mkdir -p "${tmpHerculesDir}"

# 拷贝bin
targetBinDir="${targetDir}/classes/bin"
if [ ! -d "${targetBinDir}" ]; then
  echo "Cannot find ${targetBinDir}!" 1>&2
  exit 1
fi
cp -r "${targetBinDir}" "${tmpHerculesDir}/"

# 拷贝jar
if [ ! -f "${fatjarPos}" ]; then
  echo "Cannot find ${fatjarPos}!" 1>&2
  exit 1
fi
cp "${fatjarPos}" "${tmpHerculesDir}/"

# 赋权
chmod -R 755 "${tmpHerculesDir}/"
# 打包
tar -pczvf "${targetDir}/${tarName}.tar.gz" -C "${tmpDir:?}" "${finalDirName}"
# 删除临时文件夹
rm -rf "${tmpDir:?}/"
