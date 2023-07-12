if ENV["INSIDE_EMACS"] then
   IRB.conf[:USE_MULTILINE] = nil
   IRB.conf[:USE_SINGLELINE] = false
   IRB.conf[:PROMPT_MODE] = :INF_RUBY

   IRB.conf[:USE_READLINE] = false
   IRB.conf[:USE_COLORIZE] = true
end
