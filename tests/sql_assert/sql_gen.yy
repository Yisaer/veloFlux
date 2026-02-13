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


agg_fns_int = { "sum", "min", "max", "count", "last_row" }

function rand_pos_int(minv, maxv)
  return tostring(math.random(minv, maxv))
end

function having_pred()
  return agg_call() .. " " .. pick(int_ops) .. " " .. rand_int()
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

function window_expr()
  return "countwindow(" .. rand_pos_int(2, 6) .. ")"
end

function group_by_list()
  local parts = {}
  table.insert(parts, window_expr())
  if #int_cols > 0 and math.random(2) == 1 then
    table.insert(parts, pick_int_col())
  end
  return table.concat(parts, ", ")
end

function agg_call()
  local fn = pick(agg_fns_int)
  local col = pick_int_col()
  return fn .. "(" .. col .. ")"
end

function agg_select_list()
  local cols = {}
  table.insert(cols, agg_call() .. " AS s")

  if #int_cols > 0 and math.random(2) == 1 then
    table.insert(cols, pick_int_col() .. " AS k")
  end

  return table.concat(cols, ", ")
end

function having_expr()
  local depth = math.random(1, 3)
  local out = agg_call()
  for i = 2, depth do
    out = out .. " " .. pick(logic_ops) .. " " .. having_pred()
  end
  return out
end

function order_by_clause()
  return "s " .. pick(order_dirs)
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
  | ORDER BY order_items

order_items:
  order_item
  | order_item , order_items

order_item:
  {print(pick(all_cols))} {print(pick(order_dirs))}

agg_select_stmt:
  SELECT {print(agg_select_list())}
  FROM {{table_name}}
  where_opt
  GROUP BY {print(group_by_list())}
  having_opt
  agg_order_opt

having_opt:
  | HAVING {print(having_expr())}

agg_order_opt:
  | ORDER BY {print(order_by_clause())}

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
