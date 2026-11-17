# Scripted recording of `make demo` -> docs/demo.gif. Re-record with:
#   make demo-stack && vhs docs/demo.tape
# (needs vhs: `brew install vhs`). The stack is started off camera so the GIF skips the image build.
Output docs/demo.gif

Set Shell "bash"
Set FontSize 15
Set Width 1180
Set Height 820
Set Padding 18
Set TypingSpeed 45ms
Set Theme "Catppuccin Mocha"
Set WindowBar Colorful

Hide
Type "export PS1='$ ' && make demo-stack && clear"
Enter
Wait@180s /^\$$/
Show

Type "make demo"
Sleep 400ms
Enter
Wait+Screen@240s /make down/
Sleep 4s
