#!/usr/bin/awk -f
# Emit the multiset of BODY code lines: comments, blanks, `mod` decls, `impl`
# headers and WHOLE `use` blocks (including their continuation lines, which the
# naive filter left behind and which produced the only noise in the first pass)
# are dropped, and the visibility prefix is normalised away so a split's
# mechanically-required `pub(...)` restatement does not read as a body change.
# What survives must be byte-identical before and after a pure move.
{
    line = $0
    sub(/^[ \t]+/, "", line)
    sub(/[ \t]+$/, "", line)
}
in_use == 1 {
    if (line ~ /;$/) { in_use = 0 }
    next
}
line ~ /^use / {
    if (line !~ /;$/) { in_use = 1 }
    next
}
line == "" { next }
line ~ /^\/\// { next }
line ~ /^mod / { next }
line == "impl ConversationAuthority {" { next }
{
    # normalise visibility: pub, pub(crate), pub(super), pub(in path::to::mod)
    sub(/^pub\(in [a-zA-Z0-9_:]+\) /, "", line)
    sub(/^pub\(crate\) /, "", line)
    sub(/^pub\(super\) /, "", line)
    sub(/^pub /, "", line)
    print line
}
