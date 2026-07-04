
Easiest DIY version: Hammerspoon

This is probably the fastest route. Hammerspoon is a macOS automation tool that lets Lua scripts use system APIs; its setup asks you to grant Accessibility permission. It has APIs to query the current keyboard layout/source ID, observe keyboard events, and speak text through macOS text-to-speech.

Just make sure you provide permissions to hammerspoon to operate (Settings -> accessibility )

Install Hammerspoon
```bash
brew update
brew install --cask hammerspoon
```

open its config, and paste this into ~/.hammerspoon/init.lua:

```Lua

-- Speak the current macOS input source when typing starts.
-- Put this in ~/.hammerspoon/init.lua, then Reload Config.

local speech = hs.speech.new()
speech:rate(230)
speech:volume(0.45)

local idleAfterSeconds = 2.0
local lastKeyTime = 0
local lastSpokenSourceID = nil

-- Customize these after checking your exact source IDs.
-- In the Hammerspoon console, run:
--   hs.keycodes.layouts(true)
--   hs.keycodes.methods(true)
local sourceLabels = {
  ["com.apple.keylayout.US"] = "English",
  ["com.apple.keylayout.ABC"] = "English",
  ["com.apple.keylayout.British"] = "English",

  ["com.apple.keylayout.Hebrew"] = "Hebrew",

  ["com.apple.keylayout.German"] = "German",
  ["com.apple.keylayout.German-PC"] = "German",
}

local function currentLanguageLabel()
  local sourceID = hs.keycodes.currentSourceID() or ""
  local layoutName = hs.keycodes.currentLayout()
  local methodName = hs.keycodes.currentMethod()
  local fallbackName = layoutName or methodName or sourceID or "Unknown"

  if sourceLabels[sourceID] then
    return sourceLabels[sourceID], sourceID
  end

  -- Fallback heuristics, useful until you fill in the exact IDs above.
  local haystack = string.lower(sourceID .. " " .. fallbackName)

  if string.find(haystack, "hebrew") then
    return "Hebrew", sourceID
  elseif string.find(haystack, "german") then
    return "German", sourceID
  elseif string.find(haystack, "abc") or string.find(haystack, "u%.s") or string.find(haystack, "british") then
    return "English", sourceID
  end

  return fallbackName, sourceID
end

local function isLikelyTyping(event)
  -- Avoid announcing when using shortcuts like Cmd-Tab, Cmd-C, Option-Left, etc.
  local flags = event:getFlags()
  if flags.cmd or flags.ctrl or flags.alt or flags.fn then
    return false
  end

  -- Ignore navigation and escape keys.
  local code = event:getKeyCode()
  local ignored = {
    [53] = true,  -- Escape
    [123] = true, -- Left arrow
    [124] = true, -- Right arrow
    [125] = true, -- Down arrow
    [126] = true, -- Up arrow
    [115] = true, -- Home
    [116] = true, -- Page Up
    [119] = true, -- End
    [121] = true, -- Page Down
  }

  return not ignored[code]
end

local function say(text)
  if speech:speaking() then
    speech:stop("immediate")
  end
  speech:speak(text)
end

typingLanguageWatcher = hs.eventtap.new(
  { hs.eventtap.event.types.keyDown },
  function(event)
    -- In password fields and similar contexts, macOS may enable Secure Input.
    -- In that case, keyboard events may not be sent to event taps.
    if hs.eventtap.isSecureInputEnabled() then
      return false
    end

    if not isLikelyTyping(event) then
      return false
    end

    local now = hs.timer.secondsSinceEpoch()
    local label, sourceID = currentLanguageLabel()

    local startingAfterIdle = (now - lastKeyTime) > idleAfterSeconds
    local sourceChanged = sourceID ~= lastSpokenSourceID

    if startingAfterIdle or sourceChanged then
      say(label)
      lastSpokenSourceID = sourceID
    end

    lastKeyTime = now
    return false
  end
)

typingLanguageWatcher:start()

hs.alert.show("Input-source speaker loaded")
```

That gives you this behavior: pause for 2 seconds, start typing, hear “English,” “Hebrew,” or “German.” If you switch input sources while already typing, it should announce the new one on the next typed key.

To tune it, change idleAfterSeconds, speech:volume(...), and the sourceLabels map. The exact layout IDs vary by keyboard source, so run hs.keycodes.layouts(true) in the Hammerspoon console and map whatever IDs you actually use.

press the hammerspoon icon and choose “Reload Config” to load the script. You should see an alert confirming it’s loaded, and then it should start announcing your input source as you type.
