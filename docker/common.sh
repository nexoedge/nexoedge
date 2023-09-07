TAG=latest
DOCKER='sudo -E docker'

NC='\033[0m'
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'

error() {
  echo -e "${RED}${1}${NC}"
}

info() {
  echo -e "${GREEN}${1}${NC}"
}
