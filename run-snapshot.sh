set -e

url=$1

rm -rf target/checkout
git clone --mirror --filter=blob:none "${url}" target/checkout