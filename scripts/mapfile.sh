#!/bin/bash

## define mapfile

if ! (enable | grep -q 'enable mapfile'); then
  function mapfile() {
    local    DELIM="${DELIM-$'\n'}";     opt_d() {    DELIM="$1"; }
    local    COUNT="${COUNT-"0"}";       opt_n() {    COUNT="$1"; }
    local   ORIGIN="${ORIGIN-"0"}";      opt_O() {   ORIGIN="$1"; }
    local     SKIP="${SKIP-"0"}";        opt_s() {     SKIP="$1"; }
    local    STRIP="${STRIP-"0"}";       opt_t() {    STRIP=1;    }
    local  FROM_FD="${FROM_FD-"0"}";     opt_u() {  FROM_FD="$1"; }
    local CALLBACK="${CALLBACK-}";       opt_C() { CALLBACK="$1"; }
    local  QUANTUM="${QUANTUM-"5000"}";  opt_c() {  QUANTUM="$1"; }

    unset OPTIND; local extra_args=()
    while getopts ":d:n:O:s:tu:C:c:" opt; do
      case "$opt" in
        :)  echo "${FUNCNAME[0]}: option '-$OPTARG' requires an argument" >&2; exit 1 ;;
       \?)  echo "${FUNCNAME[0]}: ignoring unknown argument '-$OPTARG'" >&2 ;;
        ?)  "opt_${opt}" "$OPTARG" ;;
      esac
    done

    shift "$((OPTIND - 1))"; set -- ${extra_args[@]+"${extra_args[@]}"} "$@"

    local var="${1:-MAPFILE}"

    ### Bash 3.x doesn't have `declare -g` for "global" scope...
    eval "$(printf "%q" "$var")=()" 2>/dev/null || { echo "${FUNCNAME[0]}: '$var': not a valid identifier" >&2; exit 1; }

    local __skip="${SKIP:-0}" __counter="${ORIGIN:-0}"  __count="${COUNT:-0}"  __read="0"

    ### `while read; do...` has trouble when there's no final newline,
    ### and we use `$REPLY` rather than providing a variable to preserve
    ### leading/trailing whitespace...
    while true; do
      if read -d "$DELIM" -r <&"$FROM_FD"
         then [[ ${STRIP:-0} -ge 1 ]] || REPLY="$REPLY$DELIM"
         elif [[ -z $REPLY ]]; then break
      fi

      (( __skip-- <= 0 )) || continue
      ((  COUNT <= 0 || __count-- > 0 )) || break

      ### Yes, eval'ing untrusted content is insecure, but `mapfile` allows it...
      if [[ -n $CALLBACK ]] && (( QUANTUM > 0 && ++__read % QUANTUM == 0 ))
         then eval "$CALLBACK $__counter $(printf "%q" "$REPLY")"; fi

      ### Bash 3.x doesn't allow `printf -v foo[0]`...
      ### and `read -r foo[0]` mucks with whitespace
      eval "${var}[$((__counter++))]=$(printf "%q" "$REPLY")"
    done
  }
fi
