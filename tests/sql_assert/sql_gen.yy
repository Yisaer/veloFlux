{
all_cols = { {{all_cols}} }
int_cols = { {{int_cols}} }
bool_cols = { {{bool_cols}} }
str_cols = { {{str_cols}} }

math.randomseed({{seed}})

int_ops = { "=", "!=", "<", "<=", ">", ">=" }
eq_ops = { "=", "!=" }
logic_ops = { "AND", "OR" }
order_dirs = { "ASC", "DESC" }

agg_fns_int = { "sum", "count"}

function rand_pos_int(minv, maxv)
  return tostring(math.random(minv, maxv))
end

function having_pred()
  local fn = pick(agg_fns_int)
  local col = pick_int_col()

  if fn == "count" then
    local op = pick(int_ops)
    local rhs
    if op == "<" then
      rhs = tostring(math.random(1, 10))
    else
      rhs = tostring(math.random(0, 10))
    end
    return "count(" .. col .. ") " .. op .. " " .. rhs
  end

  return "sum(" .. col .. ") " .. pick(int_ops) .. " " .. rand_int()
end

function pick(list)
  return list[math.random(#list)]
end

function pick_cols(list)
  local n = math.random(#list)
  local used = {}
  local out = {}
  while #out < n do
    local col = list[math.random(#list)]
    if used[col] == nil then
      used[col] = true
      table.insert(out, col)
    end
  end
  return table.concat(out, ", ")
end

function rand_int()
  local min = -10
  local max = 10
  return tostring(math.random(min, max))
end

function rand_bool()
  if math.random(2) == 1 then
    return "true"
  end
  return "false"
end

function rand_str()
  local chars = "abcdef"
  local len = math.random(1, 4)
  local out = ""
  for i = 1, len do
    local idx = math.random(#chars)
    out = out .. string.sub(chars, idx, idx)
  end
  return "'" .. out .. "'"
end

function pred_int()
  return pick(int_cols) .. " " .. pick(int_ops) .. " " .. rand_int()
end

function pred_bool()
  return pick(bool_cols) .. " " .. pick(eq_ops) .. " " .. rand_bool()
end

function pred_str()
  return pick(str_cols) .. " " .. pick(eq_ops) .. " " .. rand_str()
end

function pred_any()
  local choice = math.random(3)
  if choice == 1 then
    return pred_int()
  elseif choice == 2 then
    return pred_bool()
  end
  return pred_str()
end


function pick_int_col()
  return pick(int_cols)
end

function agg_call()
  local fn = pick(agg_fns_int)
  local col = pick_int_col()
  return fn .. "(" .. col .. ")"
end

current_group_key = nil

function group_by_list()
  return current_group_key
end

function agg_select_list()
  if current_group_key == nil then
    current_group_key = pick_int_col()
  end
  local cols = {}
  table.insert(cols, agg_call() .. " AS s")
  table.insert(cols, current_group_key .. " AS k")
  return table.concat(cols, ", ")
end

function having_expr()
  return having_pred()
end

function order_by_clause()
  return "s " .. pick(order_dirs)
end

function order_items_unique()
  local maxn = math.min(3, #all_cols)
  local n = math.random(1, maxn)
  local used = {}
  local out = {}
  while #out < n do
    local col = pick(all_cols)
    if used[col] == nil then
      used[col] = true
      table.insert(out, col .. " " .. pick(order_dirs))
    end
  end
  return table.concat(out, ", ")
end

function agg_order_items_unique()
  local keys = {"s", "k", current_group_key}

  local maxn = math.min(3, #keys)
  local n = math.random(1, maxn)
  local used = {}
  local out = {}
  while #out < n do
    local key = pick(keys)
    if used[key] == nil then
      used[key] = true
      table.insert(out, key .. " " .. pick(order_dirs))
    end
  end
  return table.concat(out, ", ")
end

}


query:
  select_stmt
  | agg_select_stmt

select_stmt:
  SELECT select_cols FROM {{table_name}} where_opt order_opt

where_opt:
  | WHERE expr

order_opt:
  | ORDER BY {print(order_items_unique())}

agg_select_stmt:
  {current_group_key = nil}
  SELECT {print(agg_select_list())}
  FROM {{table_name}}
  where_opt
  GROUP BY {print(group_by_list())}
  having_opt
  agg_order_opt

having_opt:
  | HAVING {print(having_expr())}

agg_order_opt:
  | ORDER BY {print(agg_order_items_unique())}

select_cols:
  *
  | {print(pick_cols(all_cols))}

expr:
  predicate
  | predicate logic_op expr

predicate:
  {print(pred_any())}

logic_op:
  {print(pick(logic_ops))}
