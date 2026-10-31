export const dynamic='force-dynamic';
export function GET(){return Response.json({status:'alive',service:'data-horizon-ui'},{headers:{'Cache-Control':'no-store'}});}
